[riodb.co](https://www.riodb.co)  

RioDB (www.riodb.org) is a downloadable software for data stream processing.  
You can build powerful data screening bots through simple SQL-like statements.  

## In a nuthshell:

- You define streams that will receive data using plugins (udp, tcp, http, etc.)
- You define windows of data (example: messages received in the last 2 hours that contain the word 'free'.)
- You write queries against these windows, similarly to how you query tables in a database.
- Your queries also indicate what action to take when conditions are met.
- RioDB takes actions by using plugins (udp, tcp, http, etc.)

RioDB is ultra lightweight and fast so that you can crunch through a lot of data using very little compute resources.  
Plugins are open source and anyone can make their own custom plugin if they want to.  

---

This git repository contains plugins that can be loaded by RioDB during runtime.  

## Plugins

This repository contains RioDBPlugin implementations for 
- input.KAFKA
- output.KAFKA
- output.ELASTICSEARCH
- output.AWS_SNS

## Development

Plugins are developed in Java.  
The best place to start is probably our [Discord server](https://discord.gg/FbjRHstSkV)  
