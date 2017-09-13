Notes for executing streaming examples:
=

How does it work?
-
* The streaming based on file will create a queue in /tmp/streaming/in (or c:\temp\streaming\in on Windows).
* Run a streaming consumer first (e.g. StreamingIngestionFileSystemTextFileApp).
* Run once or more time the streaming producer (e.g. RandomRecordGeneratorApp).

Special notes
-
* Under Windows, do not copy files in your streaming queue as the date will not change and it will not be processed in the queue.
