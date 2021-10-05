[![N|Solid](https://www.riodb.org/images/Logo_Name_Small.jpg)](https://www.riodb.org/index.html)

RioDB (www.riodb.org) is a downloadable software for data stream processing.  
You can build powerful data screening bots through simple SQL-like statements.

## In a nuthshell:

- You define streams that will receive data using plugins (udp, tcp, http, etc.)
- You define windows of data (example: messages received in the last 2 hours that contain the word 'free'.)
- You write queries against these windows, similarly to how you query tables in a database.
- Your queries also indicate what action to take when conditions are met.
- RioDB takes actions by using plugins (udp, tcp, http, etc.)

RioDB is ultra lightweight and fast so that you can crunch through a lot of data using very little compute resources.
Plugins are also open source and anyone can make their own custom plugin if they want to. 

---

This git repository contains plugins that can be loaded by RioDB during runtime.  
If you are looking for the RioDB engine code, the repository is [RioDB/riodb](https://github.com/RioDB/riodb)


## Plugins

This repository contains RioDBPlugin implementations for 
- BENCH (input only)
- HTTP (input and output)
- TCP (input and output)
- UDP (input and output)

Instructions on how to use them are available at https://www.riodb.org/plugins.html

## Development

Want to join the cause? Awesome!  
Plugins are developed in Java.  
The best place to start is probably our [Discord server](https://discord.gg/FbjRHstSkV)  

From there, you can request/upvote new plugins to be created, or we can guide you on how code a plugin yourself.  
You can make a RioDBPlugin implementation in your own repository, even closed source.  
But we will only bundle plugins into the RioDB downloadable archive if they are opensourced in this repository.    
