module Program

open System
open System.Threading
open HttpClient
open HtmlAgilityPack
open FSharp.MongoDB.Bson.Serialization
open Suave.Web
open Suave.Http
open Suave.Http.Applicatives
open Suave.Http.Files
open Suave.Http.Successful
open Suave.Types
open System.Net
open Scraping
open Util
open FSharp.Control

let cacheInMongo client collection (f: Async<'a list>) =
    async {
        let! deletedCount = Mongo.delete<'a> client collection <@ fun x -> true @>

        let! results = f

        do! results
            |> List.map (Mongo.insert<'a> client collection)
            |> Async.InBatches 20 1000
            |> Async.Ignore
    }

module Suave =
    open Suave.Types
    open Suave.Http.Successful

    let handle f view: WebPart =
      fun (x : HttpContext) ->
        async {
          let! results = f
          return! OK (view results) x
        }

    let handleSync f view: WebPart =
      fun (x : HttpContext) ->
        async {
          let results = f ()
          return! OK (view results) x
        }

    let startUp handler: WebPart =
        fun (x: HttpContext) ->
            async {
                handler () |> Async.Start
                return Some x
            }

    let pause ms: WebPart = 
        fun (x: HttpContext) ->
            async {
                do! Async.Sleep(ms)
                return Some x
            }

module Handlers =
    let status = ref None

    let start () =
        let runSeq = doJob ApartmentJob.jobConfig

        runSeq
        |> AsyncSeq.iter (fun runState -> status := Some runState)

    let getStatus () =
        match !status with
        | Some runStatus -> runStatus
        | None -> failwith "unexpected"

module Views = 
    let viewOption noneHtml someView =
        fun option -> 
            match option with 
            | None -> noneHtml
            | Some x -> someView x

    let viewAsCode x =
        sprintf "<pre>%A</pre>" x

    let viewStepStatus (stepStatus: StepState) =
        let viewRunningStepStatus (viewInput: 'TInput -> string) (viewResult: 'TParseResult -> string) (runState: RunningStepState<'TInput, 'TParseResult, 'TOutput>) =
            let viewStatus (status: RunRowStatus<'TParseResult>) =
                match status with
                | NotStarted -> "<td colspan='2'>Not Started</td>"
                | Waiting -> "<td colspan='2'>Waiting</td>"
                | Done result -> 
                    let xx =
                        match result with
                        | FetchFail exns ->
                            sprintf "<div title=\"%s\">Fetch Failure</div>" ((sprintf "%A" exns).Replace("\"", "'"))
                        | FetchSuccess (html, parseResult) ->
                            match parseResult with
                            | ParseFail exns ->
                                sprintf "<div title=\"%s\">Parse Failure</div>" ((sprintf "%A" exns).Replace("\"", "'"))
                            | ParseSuccess parsed ->
                                parsed |> viewResult
                    sprintf "<td><div>Done</div></td><td>%s</td>" (xx)

            let viewRow (row: StepRow<'TInput, 'TParseResult>) = 
                sprintf "<tr><td>%s</td><td>%s</td></tr>" (viewInput row.Fragment) (viewStatus row.Status)

            let topRow = "<tr><th>Fragment</th><th>Status</th></tr>"
        
            sprintf "<table><thead>%s</thead><tbody>%s</tbody></table><h1>Final Results</h1><div>%s</div>" 
                topRow 
                (runState.Results |> List.map viewRow |> String.concat "")
                (runState.OverallResult |> (viewAsCode |> viewOption "None Yet..."))

        match stepStatus with 
        | WaitingForInput -> "<h2>Waiting for input from previous step...</h2>"
        | Running state -> viewRunningStepStatus viewAsCode viewAsCode state

    let viewJobStatus (jobState: JobState) = 
        "<h1>Job State</h1>"
        + (jobState.Steps |> Array.mapi (fun i step -> "<h2>Step " + (i + 1).ToString() + "</h2><div>" + (viewStepStatus step) + "</div>") |> String.concat "\n")

let client = Mongo.conn (env "MongoConnectionString")
let db = Mongo.getDb "local" client

let staticFilePath = env "StaticFilePath"

let app =
    choose
        [
            GET >>= choose [
                path "/start" >>= Suave.startUp Handlers.start >>= Suave.pause 1000 >>= Redirection.found "results"
                path "/results" >>= Suave.handleSync Handlers.getStatus Views.viewJobStatus
                path "/" >>= file (staticFilePath + "index.html")
                browse staticFilePath
            ]

            OK "Unknown URL"
        ]

[<EntryPoint>]
let main argv =
    startWebServer
        { defaultConfig with bindings = [ HttpBinding.mk HTTP (IPAddress.Parse "0.0.0.0") 8083us ] }
        app
    0
