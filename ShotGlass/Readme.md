# GlutenFree.ShotGlass.Stiff

*Hey Kids, Wanna buy some Event Storage Routines?* [0]

An opinionated yet flexible event storage/retrieval library.

## What does it do?

 - Takes parts of the core of Akka.Persistence.Sql and gives you bits to: 
    - Write of Events (with a batch queue to maximize throughput)
      - When you write Events you get a Task back. Do with the completion what you will. 
    - Read Events (with a batch-spooler)
      - Right now it only reads till it runs out of records. Polling will be added later.
    - Get the Current Maximum Sequence for an Queue
 - Stores the data in tables that you have a high degree of control over, as long as you have four required columns.
 - Wants you to hack over it:
   - Consumer-level APIs use Standard .NET Types;
     - Specialized `ChannelReader<>`s for reading
     - `Task<T>`s for writing
 - Lets you be kinda lazy:
   - You can get started just by telling it your database type and the connection string.
     - And defining your type.
## What does it not do *yet*? (Expected features)
- Provide a clear API for fluently mapping tables and sequence rules
  - The Linq2Db API is there, however this should ideally require minimal understanding of Linq2Db API to customize,
    - or have a sidecar library to assist.
- Provide a `Func<T,ValueTask> afterwrite` injectible for the write pipeline for advanced scenarios
## What Doesn't it do?

- Tell you how your events should be precisely columnized.
- Tell you how your events are sequenced
  - This means you need to provide your own sequencing, either via a DB Sequence or a wrapper.
  - **This also means there is nothing to check for a gap in sequences**
- Let you use Non-SQL Stores [1]
  - I'm not opposed to providing base classes


[0] - Apologies, a Civvie-11 Reference is a terrible way to start a Readme and yet here we are.

[1] - OK so I think you technically could use Excel via JET but that's still SQL, just with a lot of magic in between.