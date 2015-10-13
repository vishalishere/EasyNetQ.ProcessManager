module EasyNetQ.ProcessManager.Tests.ActiveStore

open EasyNetQ.ProcessManager
open EasyNetQ.ProcessManager.State.Memory
open EasyNetQ.ProcessManager.State.SqlServer
open FsCheck
open NUnit.Framework

[<Test>]
let ``Create memory active store`` () =
    MemoryActiveStore() |> ignore

type MemoryStateGenerator =
    static member IActiveState () =
        Gen.fresh (fun () -> MemoryActiveStore() :> IActiveStore) |> Arb.fromGen

let [<Literal>] ConnString = "Server=(local);Database=Messenger;Trusted_Connection=true"
type SQLStateGenerator =
    static member IActiveState () =
        Gen.fresh (fun () -> SqlActiveStore(ConnString) :> IActiveStore) |> Arb.fromGen

type Properties() =
    static member ``Add continuation should make workflow active`` (sut : IActiveStore) (cont : Active) =
        sut.Add cont
        let r = sut.WorkflowActive cont.WorkflowId
        sut.Remove (cont.CorrelationId, cont.NextStep, cont.WorkflowId)
        r
    static member ``Cancelling a workflow should remove all continuations of that workflow`` (sut : IActiveStore) (cont : Active) =
        sut.Add cont
        sut.CancelWorkflow cont.WorkflowId
        sut.WorkflowActive cont.WorkflowId |> not
    static member ``Cancelling a workflow should remove all continuations of that workflow, but not any other workflow`` (sut : IActiveStore) (cont : Active) (otherCont : Active) =
        if cont.WorkflowId <> otherCont.WorkflowId then
            sut.Add cont
            sut.Add otherCont
            sut.CancelWorkflow cont.WorkflowId
            let r = sut.WorkflowActive otherCont.WorkflowId
            sut.CancelWorkflow otherCont.WorkflowId
            r
        else true
    static member ``Continuations are returned`` (sut : IActiveStore) (conts : Active list) =
        conts |> List.iter sut.Add
        let r = conts |> List.forall (fun cont -> sut.Continuations cont.CorrelationId |> List.length > 0)
        conts |> List.iter (fun c -> sut.CancelWorkflow c.WorkflowId)
        r
    static member ``Removing processed step shouldn't remove any others`` (sut : IActiveStore) (cont : Active) (other : Active) =
        if other.NextStep = cont.NextStep then
            true
        else
            let cont' = { other with WorkflowId = cont.WorkflowId; CorrelationId = cont.CorrelationId }
            sut.Add cont
            sut.Add cont'
            sut.Remove (cont.CorrelationId, cont.NextStep, cont.WorkflowId)
            let conts = sut.Continuations cont'.CorrelationId
            let r = conts |> Seq.exists (fun (s, w) -> (s, w) = (cont'.NextStep, cont'.WorkflowId))
            sut.CancelWorkflow cont.WorkflowId
            r
    static member ``Continuations should elapse`` (sut : IActiveStore) (cont : Active) (timeOut : StepName) =
        let cont' = { cont with TimeOut = System.TimeSpan.FromMilliseconds -100.; TimeOutNextStep = Some timeOut }
        sut.Add cont'
        let processed = ref false
        let workflows = sut.ProcessElapsed (fun _ -> processed := true)
        sut.CancelWorkflow cont'.WorkflowId
        !processed |@ "Processed not triggered" .&.
        (workflows |> Seq.exists (fun w -> w = cont'.WorkflowId)) |@ "WorkflowId not returned"
    static member ``Multiple continuations on the same correlation id should be respected`` (sut : IActiveStore) (cont : Active)  =
        sut.Add cont
        sut.Add cont
        let r = sut.Continuations (cont.CorrelationId) |> List.length = 2
        sut.CancelWorkflow cont.WorkflowId
        r

[<Test>]
let ``Run properties for memory state`` () =
    Arb.register<CoreGenerators>() |> ignore
    Arb.register<MemoryStateGenerator>() |> ignore
    FsCheck.Check.QuickThrowOnFailureAll<Properties>()

[<Explicit>]
[<Test>]
let ``Run properties for sql state`` () =
    Arb.register<CoreGenerators>() |> ignore
    Arb.register<SQLStateGenerator>() |> ignore
    FsCheck.Check.QuickThrowOnFailureAll<Properties>()
