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
    static member IPMBusFunc () =
        Gen.constant (fun () -> new InProcessPMBus() :> IPMBus) |> Arb.fromGen

let ibus () = EasyNetQ.RabbitHutch.CreateBus "host=192.168.57.50;username=test;password=test"
type RabbitGenerator =
    static member IPMBusFunc () =
        Gen.constant (fun () -> new EasyNetQPMBus(ibus (), fun sc -> sc.WithAutoDelete true |> ignore) :> IPMBus) |> Arb.fromGen

type TopicAndMatchingBinding =
    {
        Topic : string
        MatchingBinding : string
    }

type TopicAndNotMatchingBinding =
    {
        Topic : string
        NotMatchingBinding : string
    }

let topicChars = List.concat[['0'..'9'];['a'..'b'];['A'..'B']]
type TopicPart = TopicPart of string
type Topic =
    Topic of TopicPart list
    with
        override x.ToString() =
            match x with Topic t -> t
            |> List.map (fun (TopicPart tp) -> tp)
            |> String.concat "."
        member x.Get =
            match x with Topic t -> t

type TransportUtilGenerator =
    static member TopicPart () =
        gen {
            let charGen = Gen.elements topicChars
            let! size = Gen.choose (1, 50)
            let! chars = Gen.listOfLength size charGen
            return 
                chars 
                |> List.map string 
                |> String.concat ""
                |> TopicPart
        }
        |> Arb.fromGen
    static member Topic () =
        gen {
                let! size = Gen.choose (1,4)
                let! parts =
                    Gen.listOfLength size Arb.generate<TopicPart>
                return Topic parts
        }
        |> Arb.fromGen
    static member TopicAndMatchingBinding () =
        gen {
            let! t = Arb.generate<Topic>
            let! change =
                Gen.listOfLength (List.length t.Get) Arb.generate<bool>
            let withStars =
                List.zip change t.Get
                |> List.map (fun (b, tp) -> if b then TopicPart "*" else tp)
                |> Topic
            let! i = Gen.choose (1, List.length t.Get - 1)
            let withHash =
                t.Get
                |> List.take i
                |> fun x -> List.concat [x;[TopicPart "#"]]
                |> Topic
            let! possible = Gen.elements [t;withStars;withHash]
            return { Topic = t.ToString(); MatchingBinding = possible.ToString() }
        }
        |> Arb.fromGen
    static member TopicAndNotMatchingBinding () =
        [
            gen {
                let! t = Arb.generate<Topic>
                let! replacement =
                    Arb.generate<TopicPart>
                    |> Gen.suchThat (fun tp -> t.Get |> List.contains tp |> not)
                let! i = Gen.choose(0, List.length t.Get - 1)
                let other =
                    t.Get
                    |> List.mapi (fun j tp -> if i = j then replacement else tp)
                    |> Topic
                return {
                        Topic = t.ToString()
                        NotMatchingBinding = other.ToString()
                    }
            }
            gen {
                let! t = Arb.generate<Topic>
                let! extra =
                    [
                        Arb.generate<TopicPart>
                        Gen.constant (TopicPart "*")
                    ]
                    |> Gen.oneof
                return {
                    Topic = t.ToString()
                    NotMatchingBinding = (List.concat [t.Get;[extra]] |> Topic).ToString()
                }
            }
            gen {
                let! t = Arb.generate<Topic>
                let! unmatchedStart =
                    Arb.generate<TopicPart>
                    |> Gen.suchThat (fun tp -> tp <> t.Get.Head)
                return {
                    Topic = t.ToString()
                    NotMatchingBinding = (Topic [unmatchedStart;TopicPart "#"]).ToString()
                }
            }
        ]
        |> Gen.oneof
        |> Arb.fromGen

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
    static member ``Publish/subscribe works`` (busFunc : unit -> IPMBus) (NonEmptyString message) =
        let bus = busFunc()
        let tcs = TaskCompletionSource<bool>()
        bus.Subscribe<string>("TransportTests", fun m -> if m = message then tcs.SetResult true)
        bus.Publish (message, TimeSpan.FromMinutes 5.)
        !!bus (tcs.Task.Wait 500)
    static member ``Routing by type works`` (busFunc : unit -> IPMBus) message1 message2 order =
        let bus = busFunc()
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
    static member ``Expired messages are not delivered`` (busFunc : unit -> IPMBus) (message : TestMessage1) =
        let bus = busFunc()
        let tcs = TaskCompletionSource<TestMessage1>()
        bus.Subscribe<TestMessage1>("TransportTests", fun m -> if m = message then tcs.SetResult m)
        bus.Publish (message, TimeSpan.FromMilliseconds -100.)
        !!bus (tcs.Task.Wait 10 |> not)
    static member ``Matching topic and binding deliver message`` (busFunc : unit -> IPMBus) (message : TopicMessage) { Topic = topic; MatchingBinding = binding } =
        let bus = busFunc()
        let tcs = TaskCompletionSource<TopicMessage>()
        bus.Subscribe<TopicMessage>("TransportTests", binding, fun m -> if m = message then tcs.SetResult m)
        bus.Publish (message, TimeSpan.FromMinutes 5., topic)
        !!bus (tcs.Task.Wait 100)
    static member ``Unmatching topic and binding does not deliver message`` (busFunc : unit -> IPMBus) (message : TopicMessage) { Topic = topic; NotMatchingBinding = binding } =
        let bus = busFunc()
        let tcs = TaskCompletionSource<TopicMessage>()
        bus.Subscribe<TopicMessage>("TransportTests", binding, fun m -> if m = message then tcs.SetResult m)
        bus.Publish (message, TimeSpan.FromMinutes 5., topic)
        !!bus (tcs.Task.Wait 10 |> not)

[<Test>]
let ``Run properties for memory bus`` () =
    FsCheck.Check.All<Properties>(
        { Config.QuickThrowOnFailure with
            Arbitrary = [typeof<CoreGenerators>;typeof<TransportUtilGenerator>;typeof<InProcessGenerator>] })
            
[<Explicit>]
[<Test>]
let ``Run properties for rabbit bus`` () =
    FsCheck.Check.All<Properties>(
        { Config.QuickThrowOnFailure with
            Arbitrary = [typeof<CoreGenerators>;typeof<TransportUtilGenerator>;typeof<RabbitGenerator>] })
