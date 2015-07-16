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

module Handlers =
    let getOverallResult statusRef = 
        match !statusRef with
        | None ->
            failwith "Listing run needs to be run first"
        | Some x -> 
            match x.OverallResult with
            | None -> 
                failwith "Listing run needs to be finished first"
            | Some x -> x

    module Listings = 
        let status = ref None

        let start () =
            let runSeq = doListingRun "cary-nc" ([1..10] |> List.map string)

            runSeq
            |> AsyncSeq.iter (fun runState -> status := Some runState)

        let getStatus () =
            match !status with
            | Some runStatus -> runStatus
            | None -> failwith "unexpected"

        let getSortedResults () =
            getOverallResult status
            |> List.sort

    module Records = 
        let status = ref None

        let start () =
            let x = getOverallResult Listings.status
            doRecordRun x                 
            |> AsyncSeq.iter (fun runState -> status := Some runState)

        let getStatus () =
            match !status with
            | Some runStatus -> runStatus
            | None -> failwith "unexpected"

        let getSortedResults () =
            getOverallResult status
            |> List.sortBy (fun ai -> ai.YearBuilt)


module Views = 
    let viewOption noneHtml someView =
        fun option -> 
            match option with 
            | None -> noneHtml
            | Some x -> someView x

    let viewAsCode x =
        sprintf "<pre>%A</pre>" x

    let viewRunStatus (viewResult: 'a -> string) (runState: RunState<'a, 'b>) =
        let viewStatus (status: RunRowStatus<'a>) =
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
        
        let viewRow (row: RunRow<'a>) = 
            sprintf "<tr><td>%s</td><td>%s</td></tr>" row.Fragment (viewStatus row.Status)
        
        let topRow = "<tr><th>Fragment</th><th>Status</th></tr>"
        
        sprintf "<table><thead>%s</thead><tbody>%s</tbody></table><h1>Final Results</h1><div>%s</div>" 
            topRow 
            (runState.Results |> List.map viewRow |> String.concat "")
            (runState.OverallResult |> (viewAsCode |> viewOption "None Yet..."))
    
    let viewApartmentList (apartments: ApartmentInfo list) =
        let viewApartment (apt: ApartmentInfo) =
            sprintf "<tr><td>%s</td><td>%s</td><td>%s</td></tr>" (apt.YearBuilt |> viewOption "Unknown" id) apt.Name apt.Address
        let rows = apartments |> List.map viewApartment |> String.concat ""
        
        let header = "<tr><th>Year</th><th>Name</th><th>Address</th></tr>"
        sprintf "<table><thead>%s</thead><tbody>%s</tbody></table>" header rows

let client = Mongo.conn (env "MongoConnectionString")
let db = Mongo.getDb "local" client

let staticFilePath = env "StaticFilePath"

let app =
    choose
        [
            GET >>= choose [
                path "/listings/start" >>= Suave.startUp Handlers.Listings.start >>= Redirection.found "results"
                path "/listings/results" >>= Suave.handleSync Handlers.Listings.getStatus (Views.viewRunStatus (sprintf "%A"))
                path "/listings/sorted" >>= Suave.handleSync Handlers.Listings.getSortedResults (String.concat "<br />\n")
                path "/records/start" >>= Suave.startUp Handlers.Records.start >>= Redirection.found "results"
                path "/records/results" >>= Suave.handleSync Handlers.Records.getStatus (Views.viewRunStatus (sprintf "%A"))
                path "/records/sorted" >>= Suave.handleSync Handlers.Records.getSortedResults Views.viewApartmentList
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
