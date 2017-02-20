using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace Test2
{

    class Program
    {
        public static BatchBlock<T> CreateBatchBlockWithCleanup<T>(int batchSize)
        {
            var batchBlock = new BatchBlock<T>(batchSize);

            batchBlock.Completion.ContinueWith(delegate
            {
                if (batchBlock.OutputCount > 0)
                {
                    Console.WriteLine("Smart batch triggering remaining batch of leftovers");
                    batchBlock.TriggerBatch();
                }
            });

            return batchBlock;
        }

        public static IPropagatorBlock<T, T> CreateGuaranteedBroadcastBlock<T>(IEnumerable<ITargetBlock<T>> targets, bool propagateCompletion)
        {
            var source = new BufferBlock<T>();

            var target = new ActionBlock<T>(async item =>
            {
                foreach (var targetBlock in targets)
                {
                    await targetBlock.SendAsync(item);
                }
            }, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 1
            });

            if (propagateCompletion)
            {
                target.Completion.ContinueWith(async delegate
                {
                    foreach (var targetBlock in targets)
                    {
                        targetBlock.Complete();
                        await targetBlock.Completion;
                    }
                });
            }

            return DataflowBlock.Encapsulate(target, source);
        }

        static void Main(string[] args)
        {

            var mainSource = new BufferBlock<int>();

            var batchBlock = CreateBatchBlockWithCleanup<int>(5);

            var singleAction1 = new ActionBlock<int>((item) =>
            {
                Thread.Sleep(200);
                Console.WriteLine($"Single action1 received {item}");
            });

            var guaranteedBroadcastBlock =
                CreateGuaranteedBroadcastBlock<int>(new ITargetBlock<int>[] {singleAction1, batchBlock}, true);

            var batchAction = new ActionBlock<IEnumerable<int>>((items) =>
            {
                Thread.Sleep(400);
                Console.WriteLine($"Received {items.Count()} items");
            });

            mainSource.LinkTo(guaranteedBroadcastBlock, new DataflowLinkOptions()
            {
                PropagateCompletion = true
            });

            guaranteedBroadcastBlock.LinkTo(singleAction1, new DataflowLinkOptions()
            {
                PropagateCompletion = true
            });
            guaranteedBroadcastBlock.LinkTo(batchBlock, new DataflowLinkOptions()
            {
                PropagateCompletion = true
            });
            batchBlock.LinkTo(batchAction);

            foreach (var i in Enumerable.Range(1, 19))
            {
                mainSource.Post(i);
            }
            mainSource.Complete();
            mainSource.Completion.Wait();

            Console.WriteLine("Finished setup");
            Console.ReadLine();
        }
    }
}
