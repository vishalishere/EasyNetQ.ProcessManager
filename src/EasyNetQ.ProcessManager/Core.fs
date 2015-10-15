namespace EasyNetQ.ProcessManager

open System
open System.Collections.Concurrent
open System.Collections.Generic
open EasyNetQ
open EasyNetQ.FluentConfiguration

type CorrelationId = CorrelationId of string
type WorkflowId = WorkflowId of Guid
type StepName = StepName of string

/// A type keyed store of items associated with a particular
/// instance of a workflow.
///
/// Implementations of this interface take responisibility for
/// ensuring operations invoked are atomic.
type IState =
    /// Add a new item of type 'a to the state store if there
    /// is not an item of this type already associated with the
    /// workflow, or update it with the provided function if 
    /// there is.
    abstract member AddOrUpdate<'a> : 'a -> Func<'a, 'a> -> 'a
    /// Get the item of type 'a associated with this workflow
    /// or add a new item if there isn't one.
    /// Returns the item that is stored at the end of the operation.
    abstract member GetOrAdd<'a> : 'a -> 'a
    /// Get the item of type 'a that is associatied with this
    /// workflow, returning an option type as it might not exist.
    abstract member Get<'a> : unit -> 'a option

/// A store that knows how to store/retrieve the relevant IState for
/// a particular WorkflowId
type IStateStore =
    abstract member Add : WorkflowId -> IState -> unit
    abstract member Remove : WorkflowId -> unit
    abstract member Get : WorkflowId -> IState
    abstract member Create : WorkflowId -> IState

type Active =
    {
        CorrelationId : CorrelationId
        NextStep : StepName
        WorkflowId : WorkflowId
        TimeOut : TimeSpan
        TimeOutNextStep : StepName option
    }

type Elapsed =
    {
        TimedOut : StepName
        NextStep : StepName
        WorkflowId : WorkflowId
    }

/// A store of "active" continuations that a process manager is waiting to
/// callback when a message arrives.
type IActiveStore =
    /// Add a continuation to the store.
    abstract member Add<'a> : Active -> unit
    /// Remove all continuations related to the CorrelationId and type from the store.
    abstract member Remove<'a> : CorrelationId * StepName * WorkflowId -> unit
    /// Get all continuations for type 'a and the given CorrelationId.
    abstract member Continuations<'a> : CorrelationId -> (StepName * WorkflowId) list
    /// Check if there are any outstanding continuations active for the given WorkflowId.
    abstract member WorkflowActive : WorkflowId -> bool
    /// Remove all continuations associated with a WorkflowId.
    abstract member CancelWorkflow : WorkflowId -> unit
    /// For each expired continuation in the store, apply the elapsed function.
    /// Implementations of this interface must guarantee that the processing of each
    /// continuation is atomic.
    abstract member ProcessElapsed : (Elapsed -> unit) -> WorkflowId seq

/// Interface to represent the transport mechanism used to provide type based
/// routing. Optionally allows for topics to be specified.
type IPMBus =
    inherit IDisposable
    /// Publish a message of type 'a with an expiring time after which the
    /// transport guarantees it will not be delivered.
    abstract member Publish<'a when 'a : not struct> : ('a * TimeSpan) -> unit
    /// Publish a message of type 'a with an expiring time after which the
    /// transport guarantees it will not be delivered, and a topic to allow
    /// topic based subscribers to recieve it.
    abstract member Publish<'a when 'a : not struct> : ('a * TimeSpan * string) -> unit
    /// Subscribe to all messages of type 'a and call the action when they arrive
    abstract member Subscribe<'a when 'a : not struct> : string * Action<'a> -> unit
    /// Subscribe to messages of type 'a and a matching topic, and call the action when they arrive
    abstract member Subscribe<'a when 'a : not struct> : string * string * Action<'a> -> unit

[<AutoOpen>]
module private ExpiringPublish =
    let publish<'a when 'a : not struct> (bus : IBus) (dms : IMessageDeliveryModeStrategy) (ped : Producer.IPublishExchangeDeclareStrategy) message (expire : TimeSpan) topic =
        let messageType = typeof<'a>
        let props = MessageProperties(Expiration = (expire.TotalMilliseconds |> int |> string), DeliveryMode = dms.GetDeliveryMode(messageType))
        let enqMessage = Message<'a>(message, props)
        let exchange = ped.DeclareExchange(bus.Advanced, messageType, Topology.ExchangeType.Topic)
        let finalTopic =
            match topic with
            | Some t -> t
            | None -> bus.Advanced.Conventions.TopicNamingConvention.Invoke messageType
        bus.Advanced.Publish(exchange, finalTopic, false, false, enqMessage)

/// Default implementation of IPMBus, using EasyNetQ. This will preserve any custom
/// configuration you have applied to the IBus using EasyNetQ's IoC container, such
/// as overriding exchange conventions.
type EasyNetQPMBus (bus : IBus) =
    let dms = bus.Advanced.Container.Resolve<IMessageDeliveryModeStrategy>()
    let ped = bus.Advanced.Container.Resolve<Producer.IPublishExchangeDeclareStrategy>()
    interface IPMBus with
        member __.Publish<'a when 'a : not struct> ((message : 'a, expire)) =
            publish<'a> bus dms ped message expire None
        member __.Publish<'a when 'a : not struct> ((message : 'a, expire, topic)) =
            publish<'a> bus dms ped message expire (Some topic)
        member __.Subscribe<'a when 'a : not struct> (subscriptionId, action) =
            bus.Subscribe<'a>(subscriptionId, action) |> ignore
        member __.Subscribe<'a when 'a : not struct> (subscriptionId, topic, action: Action<'a>): unit = 
            bus.Subscribe<'a>(subscriptionId, action, Action<ISubscriptionConfiguration>(fun x -> x.WithTopic topic |> ignore)) |> ignore
    interface IDisposable with
        member __.Dispose() =
            bus.Dispose()

type IRequestInfo =
    abstract member Type : Type
    abstract member Message : obj
    abstract member Topic : string option
    abstract member TimeOut : TimeSpan

type internal IRequest =
    inherit IRequestInfo
    abstract member Publish : IPMBus -> unit

type internal Request<'a when 'a : not struct> =
    | Request of 'a * TimeSpan
    | TopicRequest of 'a * TimeSpan * string
    interface IRequest with
        member x.Publish (bus : IPMBus) =
            match x with
            | Request(request, expiry) ->
                bus.Publish<'a> ((request, expiry))
            | TopicRequest (request, expiry, topic) ->
                bus.Publish<'a> ((request, expiry, topic))
    interface IRequestInfo with
        member __.Type =
            typeof<'a>
        member x.Message =
            match x with
            | Request (m, _) -> box m
            | TopicRequest (m, _, _) -> box m
        member x.Topic =
            match x with
            | Request _ -> None
            | TopicRequest (_, _, t) -> Some t
        member x.TimeOut =
            match x with
            | Request (_, t) -> t
            | TopicRequest (_, t, _) -> t

type IContinuationInfo =
    abstract member Handler : string
    abstract member TimeOut : TimeSpan
    abstract member TimeOutHandler : string option
    abstract member Type : Type
    abstract member CorrelationId : string

type internal IContinuation =
    inherit IContinuationInfo
    abstract member AddActive : IActiveStore * WorkflowId -> unit

type internal Continuation<'message when 'message : not struct> (correlationId, handler, timeOut, timeOutHandler) =
    new (correlationId, handler) =
        Continuation(correlationId, handler, TimeSpan.FromDays(1.0), None)
    interface IContinuation with
        member __.AddActive (store : IActiveStore, workflowId) =
            store.Add<'message> {
                CorrelationId = CorrelationId correlationId
                NextStep = StepName handler
                WorkflowId = workflowId
                TimeOut = timeOut
                TimeOutNextStep = Option.map StepName timeOutHandler
            }
    interface IContinuationInfo with
        member __.Handler =
            handler
        member __.TimeOut =
            timeOut
        member __.TimeOutHandler =
            timeOutHandler
        member __.Type = 
            typeof<'a>
        member __.CorrelationId = 
            correlationId

type internal CallBacks =
    | Cancel
    | Conts of IContinuation list

type internal OutInternals =
    {
        Requests : IRequest list
        Continuations : CallBacks
    }

/// Type representing the output of a workflow step: the messages that should be
/// published, and the continuations that should be expected.
type Out() =
    member val internal Mine = { Requests = []; Continuations = Conts [] } with get, set
    /// Notify the process manager that this workflow should be ended immediately,
    /// and no further continuations should be processed.
    static member End =
        Out(Mine = { Requests = []; Continuations = Cancel })
    /// There is no need to take any further actions as a result of this workflow step.
    static member Ignore = Out(Mine = { Requests = []; Continuations = Conts [] })
    /// There is no need to take any further actions as a result of this workflow step.
    static member Empty = Out.Ignore
    /// Add a "request" to the workflow output. This request will be published.
    static member AddR r expiry (out : Out) =
        Out(Mine = { out.Mine with Requests = Request (r, expiry) :> IRequest::out.Mine.Requests })
    /// Add a topic based "request" to be published as a result of this workflow step.
    static member AddTopicR r expiry topic (out : Out) =
        Out(Mine = { out.Mine with Requests = TopicRequest (r, expiry, topic) :> IRequest::out.Mine.Requests })
    /// Add a continuation to be expected as a result of this workflow step.
    /// You cannot both add a continuation and cancel a workflow in the same workflow step.
    static member AddC<'response when 'response : not struct> correlationId handler timeOut timeOutHandler (out : Out) =
        let c = Continuation<'response>(correlationId, handler, timeOut, timeOutHandler)
        match out.Mine with
        | { Continuations = Cancel } -> failwith "You cannot add continuations to a workflow that is being cancelled."
        | { Continuations = Conts cs } -> Out(Mine = { out.Mine with Continuations = Conts <| (c :> IContinuation)::cs })
    /// Add a "request" to the workflow output. This request will be published.
    member x.AddRequest<'message when 'message : not struct> (m, expiry) = Out.AddR<'message> m expiry x
    /// Add a topic based "request" to be published as a result of this workflow step.
    member x.AddTopicRequest<'message when 'message : not struct> (m : 'message, expiry, topic) = Out.AddTopicR m expiry topic x
    /// Add a continuation to be expected as a result of this workflow step.
    /// You cannot both add a continuation and cancel a workflow in the same workflow step.
    /// A TimeOutMessage will be published to be handled by timeOutHandler if the expected
    /// message does not arrive within the timeOut TimeSpan.
    member x.AddCont<'response when 'response : not struct> (correlationId, handler, timeOut, timeOutHandler) =
        Out.AddC<'response> correlationId handler timeOut (Some timeOutHandler) x
    /// Add a continuation to be expected as a result of this workflow step.
    /// You cannot both add a continuation and cancel a workflow in the same workflow step.
    member x.AddCont<'response when 'response : not struct> (correlationId, handler, timeOut) =
        Out.AddC<'response> correlationId handler timeOut None x
    /// Read only sequence of requests in this instance
    member x.Requests () =
        x.Mine.Requests |> Seq.map (fun r -> r :> IRequestInfo)
    /// Read only sequence of continuations in this instance
    member x.Continuations () =
        match x.Mine.Continuations with
        | Conts cs -> cs |> Seq.map (fun c -> c :> IContinuationInfo)
        | Cancel -> Seq.empty
    /// Check if this Out will cancel the workflow
    member x.WillCancel =
        match x.Mine.Continuations with
        | Conts _ -> false
        | Cancel -> true

type private DispatchMap<'message when 'message : not struct>() = 
    let storage = Dictionary<string, 'message -> IState -> Out>()
    member __.TryGetAction name =
        storage.TryGetValue name
    member __.AddAction(name, action) =
        storage.Add(name, action)
    new(name, action) as this =
        DispatchMap()
        then this.AddAction(name, action)

type private IProcessor =
    abstract member ExtractCorrelationId : obj -> string

type private Processor<'message when 'message : not struct> =
    {
        ExtractCorrelationId : 'message -> string
        DispatchMap : DispatchMap<'message>
    }
    interface IProcessor with
        member x.ExtractCorrelationId (o : obj) =
            x.ExtractCorrelationId (unbox o)

/// A mapping from a Key string to a function that
/// knows how to handle messages of type 'a
type Mapping<'a when 'a : not struct> =
    {
        Key : string
        Func : Func<'a, IState, Out>
    }

type private SubscriptionManager (bus : IPMBus, subscriptionId, activeStore : IActiveStore, stateStore : IStateStore) =
    let pc = ConcurrentDictionary<Type, obj>()
    member private __.receive<'a when 'a : not struct> (a :  'a) =
        let activateProcessor (p: Processor<'a>) =
            let cid = p.ExtractCorrelationId a |> CorrelationId
            let workflowSteps =
                activeStore.Continuations<'a> cid
                |> List.map (fun (next, workflowId) ->
                    match p.DispatchMap.TryGetAction (match next with StepName name -> name) with
                    | true, f ->
                        let output = f a (stateStore.Get workflowId)
                        match output.Mine.Continuations with
                        | Conts cs ->
                            cs |> Seq.iter (fun c -> c.AddActive (activeStore, workflowId))
                        | Cancel ->
                            activeStore.CancelWorkflow workflowId
                            stateStore.Remove workflowId
                        output.Mine.Requests
                        |> Seq.iter (fun p -> p.Publish bus)
                        workflowId, next
                    | false, _ -> failwithf "Active correlation ID %A, but no handler for %A in the subscription manager" cid next )
            cid, workflowSteps
        match pc.TryGetValue typeof<'a> with
        | true, (:? (Processor<'a> []) as ps) ->
            ps
            |> Seq.map activateProcessor
            |> Seq.map (fun (cid, workflowSteps) -> workflowSteps |> Seq.map (fun (wid, n) -> cid, n, wid))
            |> Seq.concat
            |> Seq.map (fun (cid, n, wid) -> activeStore.Remove<'a> (cid, n, wid); wid)
            |> Seq.distinct
            |> Seq.iter (fun wid -> if not (activeStore.WorkflowActive wid) then stateStore.Remove wid)
        | _ -> ()
    member private x.AddProcessor<'message when 'message : not struct> (processor : Processor<'message>) =
        if
                pc.AddOrUpdate((typeof<'message>), box [|processor|], fun _ old -> Array.concat [unbox old;[|processor|]] |> box)
                :?> Processor<'message> []
                |> Array.length
                |> (=) 1 then
            bus.Subscribe<'message> (subscriptionId, Action<'message>(x.receive))
    static member Add extractCorrelationId (dispatchMappings : #seq<_>) (sm : SubscriptionManager) =
        let dispatchMap = DispatchMap()
        dispatchMappings
        |> Seq.iter (fun (key, f) -> dispatchMap.AddAction(key, f))
        sm.AddProcessor ({ ExtractCorrelationId = extractCorrelationId; DispatchMap = dispatchMap })
    member x.AddProcessor<'message when 'message : not struct> (extractCorrelationId : Func<'message, string>, dispatchMappings) =
        let mappings =
            dispatchMappings
            |> Seq.map (fun { Key = key; Func = f } -> key, (fun m s -> f.Invoke(m, s)))
        SubscriptionManager.Add extractCorrelationId.Invoke mappings x
    member x.AddProcessor<'message when 'message : not struct> (extractCorrelationId : Func<'message, string>, dispatchMapping) =
        x.AddProcessor(extractCorrelationId, [dispatchMapping])
    member private __.StartLogic (output : Out) stateId =
        match output.Mine.Continuations with
        | Cancel -> ()
        | Conts c ->
            c |> Seq.iter (fun c -> c.AddActive(activeStore, stateId))
        output.Mine.Requests
        |> Seq.iter (fun p -> p.Publish bus)
    member x.StartWorkflow(starter : Func<IState, Out>) =
        let stateId = Guid.NewGuid() |> WorkflowId
        let state = stateStore.Create stateId
        stateStore.Add stateId state
        let output = starter.Invoke state
        x.StartLogic output stateId
    member x.StartFromSubscibe<'message when 'message : not struct>(starter : Func<'message, IState, Out>, ?topic : string) =
        let handler (m : 'message) =
            let stateId = Guid.NewGuid() |> WorkflowId
            let state = stateStore.Create stateId
            stateStore.Add stateId state
            let output = starter.Invoke(m, state)
            x.StartLogic output stateId
        match topic with
        | Some t ->
            bus.Subscribe<'message>(subscriptionId, t, fun m -> handler m)
        | None ->
            bus.Subscribe<'message>(subscriptionId, fun m -> handler m)

/// Messages of this type will be published if a workflow's continuation
/// is not called before it expires and a time out handler is configured.
type TimeOutMessage =
    {
        CorrelationId : Guid
        TimedOutStep : string
    }

type private TimeManager (bus : IPMBus, activeStore : IActiveStore, stateStore : IStateStore) =
    let rand = Random()
    let publishTimeOut (e : Elapsed) =
        let cid = Guid.NewGuid()
        let a = { CorrelationId = cid.ToString() |> CorrelationId; NextStep = e.NextStep; TimeOut = TimeSpan.FromDays 1.0; TimeOutNextStep = None; WorkflowId = e.WorkflowId }
        activeStore.Add<TimeOutMessage> a
        let (StepName t) = e.TimedOut
        bus.Publish<TimeOutMessage>(({ CorrelationId = cid; TimedOutStep = t }, TimeSpan.FromHours 23.0))

    let rec removeTimedOut () =
        async {
            activeStore.ProcessElapsed publishTimeOut
            |> Seq.filter (activeStore.WorkflowActive >> not)
            |> Seq.iter stateStore.Remove

            do! Async.Sleep (rand.Next(1000,2000))
            return! removeTimedOut ()
        }
    do removeTimedOut () |> Async.Start
        
/// ProcessManager is a type designed to handle workflow management on top of a type 
/// routed messaging infrastructure. If used with EasyNetQ, it will handle all publishing
/// and subscriptions required, as well as managing expected continuations and their
/// expiry. ProcessManager will never create more than one subscription per topic per
/// type to the underlying bus, regardless of how many processes are attached to the
/// type.
type ProcessManager (bus : IPMBus, subscriptionId, activeStore, stateStore) =
    let subscriptionManager = SubscriptionManager(bus, subscriptionId, activeStore, stateStore)
    let _ = TimeManager(bus, activeStore, stateStore)
    /// Add a processor to the ProcessManager. A processor knows how to extract a correlationId
    /// from a type, and mappings from a named continuation to a .net function.
    member __.AddProcessor<'message when 'message : not struct>(extractCorrelationId, mappings : Mapping<'message> seq) =
        subscriptionManager.AddProcessor<'message>(extractCorrelationId, mappings) |> ignore
    /// Add a processor to the ProcessManager. A processor knows how to extract a correlationId
    /// from a type, and a mapping from a named continuation to a .net function.
    member __.AddProcessor<'message when 'message : not struct>(extractCorrelationId, mapping : Mapping<'message>) =
        subscriptionManager.AddProcessor<'message>(extractCorrelationId, mapping) |> ignore
    /// Add a processor to the ProcessManager. A processor knows how to extract a correlationId
    /// from a type, and a mapping from a named continuation to a .net function.
    member x.AddProcessor<'message when 'message : not struct>(extractCorrelationId : 'message -> string, mappings) =
        let ms =
            mappings
            |> Seq.map (fun (key, f) -> { Key = key; Func = Func<'message, IState, Out> (f) })
        x.AddProcessor<'message>(Func<_,_>(fun m -> extractCorrelationId m), ms)
    /// Start a workflow with the provided startFunc; an initial empty state is passed into the func,
    /// and a Out most be provided to set up the initial requests to be published and continuations to
    /// expect.
    member __.Start(startFunc) =
        subscriptionManager.StartWorkflow(startFunc)
    /// Subscribe to a specific message type on the transport, and start a new workflow when it is
    /// recieved. startFunc will be given the message and an initial, empty, IState
    member __.StartFromSubcribe<'startMessage when 'startMessage : not struct>(startFunc) =
        subscriptionManager.StartFromSubscibe<'startMessage>(startFunc)
    /// Subscribe to a specific message type and topic on the transport, and start a new workflow when it is
    /// recieved. startFunc will be given the message and an initial, empty, IState
    member __.StartFromSubcribe<'startMessage when 'startMessage : not struct>(startFunc, topic) =
        subscriptionManager.StartFromSubscibe<'startMessage>(startFunc, topic)
    interface IDisposable with
        member __.Dispose() =
            bus.Dispose()
           

