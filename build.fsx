#r "packages/build/FAKE/tools/FAKELib.dll"
open Fake
open Fake.Paket

let build () =
    build
        (fun d -> { d with Properties = ["Configuration","Release";"Optimize","True"]; Targets = ["Clean";"Build"] }) 
        ("src" @@ "EasyNetQ.ProcessManager.State.SqlServer" @@ "EasyNetQ.ProcessManager.State.SqlServer.fsproj")

let package () =
    Pack  (fun p -> { p with OutputPath = "output" })

let push () =
    let apiKey = environVarOrFail "apikey"
    Push (fun p -> { p with ApiKey = apiKey; WorkingDir = "output" })

Target "build" build
Target "package" package
Target "push" push
Target "default" id

"build"
    ==> "package"
    =?> ("push", match environVarOrNone "apikey" with Some a -> true | _ -> false)
    ==> "default"

RunParameterTargetOrDefault "target" "default"