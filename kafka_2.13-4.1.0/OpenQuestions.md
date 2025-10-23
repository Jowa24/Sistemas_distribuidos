## Open questions

---

## Solved questions

Are we allowed to use different Threads for the Central?
    -> YES!! Use one thread per role of the server


What is the EV_CP_M sending the status data or is it the engine?
    -> it is the enginge over a different kafka topic

Is the EV_CP_M the own handling the requests?
    -> no they flow over the kafak queues

Should we host a local web page to show the server console?
    - We have to, but not a webpage, terminal or any interface is also fine

How do the sockets work
    - just sending messages over a port

How does the server know about the different stations, when he is started not knowing their IP & Stuff
    - they register when created

--- 