# Linkage

Linkage is a job stream service based on server streaming gRPC.
One incoming stream and multiple outcome stream. 

# Install
go get github.com/Natata/linkage

# How to use

1. Define yourself engine
Define an engine which implement engine interface.
Engine interface defined in engine.go, look like below:
```
type Engine interface {
    // Start starts the engine, jobs would send to the engine throught the inbound channel
    Start(inbound <-chan *Job) error
    // Register register an output destination, results generated by engine would send to the outbound channel
    // the signal is used to notify down stream or service is closing
    Register(sig chan Signal) (<-chan *Job, error)
}
```

2. Initial linkage service
Iniital linkage with
- address: listen incoming message
- engine: engine implement
- grpc server options: if this service need credentials or other grpc server supported options
- codeAssert: except credential, you can use codeAssert to tell client if it the right service connected
- dial info: infomation of remote service this service will connect. Leave nil if this service not connect to any service.
- waiting function: the waiting mechanism to retry to ask job frmo remote service.

```
    addr := ":8081"
    engine := &MyEngine{}
    codeAssert := func(code linkage.Code) bool {
        return true
    }   

    di := &linkage.DialInfo{
        ConnCode:   "yo",
        Addr:       ":8080",
        Opts:       []grpc.DialOption{grpc.WithInsecure()},
        MaxAttempt: 2,
    }   

    srv, err := linkage.InitLinkage(addr, engine, []grpc.ServerOption{}, codeAssert, di, nil)
```

3. Run it

```
err := srv.Run()
```

example/ have full examples.

run ```go run example/rome/main.go``` and ```go run example/road/main.go``` in order to see what happened :)

