# TwitterETLmodule

The module grabs Twitter tweets using plain REST API.
it retrieve the incoming messages for 30 seconds or up to 100 messages, whichever comes first and put all in output buffer as long as the program run.

Once the program stop by using user interupt Ctrl + C. the program dump all the output buffer into a tsv file.

To run the program you still need twitter consumer keys and access token keys.
