module EasyNetQ.ProcessManager.Tests.Transport

open EasyNetQ.ProcessManager
open EasyNetQ.ProcessManager.Transport.InProcess
open FsCheck
open NUnit.Framework
open System
open System.Threading.Tasks

[<Test>]
let ``Create in process bus`` () =
    use __ = new InProcessPMBus()
    ()

type InProcessGenerator =
    static member IPMBus () =
        Gen.fresh (fun () -> new InProcessPMBus() :> IPMBus) |> Arb.fromGen

let ibus = EasyNetQ.RabbitHutch.CreateBus "host=192.168.57.50;username=test;password=test"
type RabbitGenerator =
    static member IPMBus () =
        Gen.fresh (fun () -> new EasyNetQPMBus(ibus) :> IPMBus) |> Arb.fromGen

type TopicAndMatchingBindings =
    {
        ExactTopic : string
        MatchingBinding : string list    
    }

type BindingAndMatchingTopics =
    {
        Binding : string
        MatchingTopics : string list
    }

type TransportUtilGenerator =
    static member TopicAndMatchingBinding () =
        let topicElements =
            gen {
                let! size = Gen.choose (1,4)
                return size            
            }
        topicElements

let (!!) (d : #IDisposable) =
    d.Dispose()
    id

type TestMessage1 =
    { Message : NonEmptyString }

type TestMessage2 =
    { Message : NonEmptyString }

type TopicMessage =
    { Message : NonEmptyString }

type Properties =
    static member ``Can subscribe`` (bus : IPMBus) =
        bus.Subscribe<string>("TransportTests", fun _ -> ())
        !!bus true
    static member ``Publish/subscribe works`` (bus : IPMBus) (NonEmptyString message) =
        let tcs = TaskCompletionSource<bool>()
        bus.Subscribe<string>("TransportTests", fun m -> if m = message then tcs.SetResult true)
        bus.Publish (message, TimeSpan.FromMinutes 5.)
        !!bus (tcs.Task.Wait 200)
    static member ``Routing by type works`` (bus : IPMBus) message1 message2 order =
        let tcs1 = TaskCompletionSource<TestMessage1>()
        let tcs2 = TaskCompletionSource<TestMessage2>()
        bus.Subscribe<TestMessage1>("TransportTests", fun m -> if m = message1 then tcs1.SetResult m)
        bus.Subscribe<TestMessage2>("TransportTests", fun m -> if m = message2 then tcs2.SetResult m)
        if order then
            bus.Publish(message2, TimeSpan.FromMinutes 5.)
            bus.Publish(message1, TimeSpan.FromMinutes 5.)
        else
            bus.Publish(message1, TimeSpan.FromMinutes 5.)
            bus.Publish(message2, TimeSpan.FromMinutes 5.)
        Task.WaitAll([|tcs1.Task :> Task;tcs2.Task :> Task|], 200) |> ignore
        !!bus (tcs1.Task.Result = message1 && tcs2.Task.Result = message2)
    static member ``Expired messages are not delivered`` (bus : IPMBus) (message : TestMessage1) =
        let tcs = TaskCompletionSource<TestMessage1>()
        bus.Subscribe<TestMessage1>("TransportTests", fun m -> if m = message then tcs.SetResult m)
        bus.Publish (message, TimeSpan.FromMilliseconds -100.)
        !!bus (tcs.Task.Wait 10 |> not)
    static member ``Topic subscribers recieve topic messages`` (bus : IPMBus) (message : TopicMessage) =
        let tcs = TaskCompletionSource<TopicMessage>()
        bus.Subscribe<TopicMessage>("TransportTests", "Topic", fun m -> if m = message then tcs.SetResult m)
        bus.Publish (message, TimeSpan.FromMinutes 5., "Topic")
        !!bus (tcs.Task.Wait 100)

[<Test>]
let ``Run properties for memory bus`` () =
    FsCheck.Check.All<Properties>(
        { Config.QuickThrowOnFailure with
            Arbitrary = [typeof<CoreGenerators>;typeof<InProcessGenerator>] })
            
[<Explicit>]
[<Test>]
let ``Run properties for rabbit bus`` () =
    FsCheck.Check.All<Properties>(
        { Config.QuickThrowOnFailure with
            Arbitrary = [typeof<CoreGenerators>;typeof<RabbitGenerator>] })
