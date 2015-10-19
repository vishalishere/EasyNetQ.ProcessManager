module EasyNetQ.ProcessManager.Tests.StateStore

open EasyNetQ.ProcessManager
open EasyNetQ.ProcessManager.State.Memory
open EasyNetQ.ProcessManager.State.SqlServer
open FsCheck
open NUnit.Framework
open System

type StoreValue =
    | String of NonEmptyString
    | Int of int

type TransformStoreValue =
    | Strings of (string -> string)
    | Ints of (int -> int)

type InitialTransformPair =
    StoreValue * TransformStoreValue

type StoreFunc = IState -> StoreValue -> unit
type GetAndCheckFunc = IState -> StoreValue -> bool
type UpdateFunc = IState -> TransformStoreValue -> StoreValue

[<Test>]
let ``Create memory state store`` () =
    MemoryStateStore() |> ignore

type TestSerializer() =
    interface ISerializer with
        member x.CanSerialize<'a> () =
            (x :> ISerializer).CanSerialize typeof<'a>
        member __.CanSerialize t =
            t = typeof<string> || t = typeof<int>
        member __.Deserialize<'a> s =
            match typeof<'a> with
            | t when t = typeof<string> -> s |> box |> unbox<'a>
            | t when t = typeof<int> -> Int32.Parse s |> box |> unbox<'a>
            | _ -> failwith "Unknown type"
        member __.Serialize<'a> (v : 'a) =
            match typeof<'a> with
            | t when t = typeof<string> -> v |> box |> unbox<string>
            | t when t = typeof<int> -> box v |> unbox<int> |> sprintf "%d"
            | _ -> failwith "Unknown type"

type MemoryStateGenerator =
    static member IStateStore () =
        Gen.fresh (fun () -> MemoryStateStore() :> IStateStore) |> Arb.fromGen

let [<Literal>] ConnString = "Server=(local);Database=Messenger;Trusted_Connection=true"
type SqlStateGenerator =
    static member IStateStore () =
        Gen.fresh (fun () -> SqlStateStore(ConnString, TestSerializer()) :> IStateStore) |> Arb.fromGen

type StateUtilGenerator =
    static member StoreFunc () : Arbitrary<StoreFunc> =
        [
            fun (s : IState) v ->
                match v with
                | String (NonEmptyString s') -> s.GetOrAdd s' |> ignore
                | Int i -> s.GetOrAdd i |> ignore
            fun s v ->
                match v with
                | String (NonEmptyString s') -> s.AddOrUpdate s' (Func<string, string>(fun _ -> s')) |> ignore
                | Int i -> s.AddOrUpdate i (Func<int, int>(fun _ -> i)) |> ignore
        ]
        |> List.map Gen.constant
        |> Gen.oneof
        |> Arb.fromGen
    static member GetAndCheckFunc () : Arbitrary<GetAndCheckFunc> =
        [
            fun (s : IState) v ->
                match v with
                | String (NonEmptyString s') ->
                    let other = Guid.NewGuid().ToString();
                    let stored = s.GetOrAdd other
                    s' = stored
                | Int i ->
                    let other =
                        if i = Int32.MaxValue then
                            0
                        else Int32.MaxValue
                    let stored = s.GetOrAdd other
                    i = stored
            fun s v ->
                match v with
                | String (NonEmptyString s') ->
                    s.Get<string>().Value = s'
                | Int i ->
                    s.Get<int>().Value = i
        ]
        |> List.map Gen.constant
        |> Gen.oneof
        |> Arb.fromGen
    static member UpdateFunc () : Arbitrary<UpdateFunc> =
        [
            fun (s : IState) update ->
                match update with
                | Strings f ->
                    let check = Guid.NewGuid().ToString()
                    let updated = s.AddOrUpdate<string> check (Func<_,_> f)
                    if updated = check then
                        failwith "Update of missing value"
                    else
                        updated |> NonEmptyString |> String
                | Ints f ->
                    let check = Int32.MaxValue
                    let updated = s.AddOrUpdate<int> check (Func<_,_> f)
                    if updated = check then
                        failwith "Update of missing value"
                    else
                        Int updated
            fun (s : IState) update ->
                match update with
                | Strings f ->
                    let mutable str = null
                    match s.TryUpdate<string> (Func<_,_> f, &str) with
                    | true -> str |> NonEmptyString |> String
                    | false -> failwith "Update of missing value"
                | Ints f ->
                    let mutable str = 0
                    match s.TryUpdate<int> (Func<_,_> f, &str) with
                    | true -> str |> Int
                    | false -> failwith "Update of missing value"
        ]
        |> List.map Gen.constant
        |> Gen.oneof
        |> Arb.fromGen
    static member Updater () : Arbitrary<InitialTransformPair> =
        [
            gen {
                let! v = Arb.generate<StoreValue>
                let t =
                    match v with
                    | Int _ -> Ints (fun i -> i + i) 
                    | String _ -> Strings (fun s -> s + s)
                return v, t
            }
        ]
        |> Gen.oneof
        |> Arb.fromGen

type Properties =
    static member ``Can create and get an IStore`` (sut : IStateStore) wid =
        sut.Create wid |> sut.Add wid
        sut.Get wid |> ignore
        sut.Remove wid
        true
    static member ``Can set a value in an IState and retrieve it``
            (sut : IStateStore) (storeFunc : StoreFunc) (checkFunc : GetAndCheckFunc) v =
        let wid = WorkflowId (Guid.NewGuid())
        let state = sut.Create wid
        storeFunc state v
        sut.Add wid state
        let r = checkFunc state v
        sut.Remove wid
        r
    static member ``Removing a workflow removes it's state``
            (sut : IStateStore) (storeFunc : StoreFunc) (checkFunc : GetAndCheckFunc) (v : StoreValue) =
        let wid = WorkflowId (Guid.NewGuid())
        let state = sut.Create wid
        storeFunc state v
        sut.Add wid state
        sut.Remove wid
        try
            let state = sut.Get wid
            let r = not <| checkFunc state v
            sut.Remove wid
            r
        with
        | _ -> true
    static member ``Removing a workflow doesn't effect other workflows``
            (sut : IStateStore) (storeFunc : StoreFunc) (checkFunc : GetAndCheckFunc) (v1, v2) =
        let wid1 = WorkflowId (Guid.NewGuid())
        let wid2 = WorkflowId (Guid.NewGuid())
        let state1 = sut.Create wid1
        let state2 = sut.Create wid2
        storeFunc state1 v1
        storeFunc state2 v2
        sut.Add wid1 state1
        sut.Add wid2 state2
        sut.Remove wid1
        let r = checkFunc (sut.Get wid2) v2
        sut.Remove wid2
        r
    static member ``Updating a value updates it``
            (sut : IStateStore) (storeFunc : StoreFunc) (updateFunc : UpdateFunc) ((initial, updater) : InitialTransformPair) (checkFunc : GetAndCheckFunc) =
        let wid = WorkflowId (Guid.NewGuid())
        let state = sut.Create wid
        storeFunc state initial
        sut.Add wid state
        let state = sut.Get wid
        let expected = updateFunc state updater
        let state = sut.Get wid
        let r = checkFunc state expected
        sut.Remove wid
        r

[<Test>]
let ``Run properties for memory state`` () =
    FsCheck.Check.All<Properties>(
        { Config.QuickThrowOnFailure with
            Arbitrary = [typeof<CoreGenerators>;typeof<StateUtilGenerator>;typeof<MemoryStateGenerator>] })
            
[<Explicit>]
[<Test>]
let ``Run properties for sql state`` () =
    FsCheck.Check.All<Properties>(
        { Config.QuickThrowOnFailure with
            Arbitrary = [typeof<CoreGenerators>;typeof<StateUtilGenerator>;typeof<SqlStateGenerator>] })
