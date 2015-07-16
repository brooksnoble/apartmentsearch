module Scraping

open HttpClient
open HtmlAgilityPack
open Util
open Util.HtmlAgility
open FSharp.Control

let rec tryPaths paths (node: HtmlNode) =
    match paths with
    | (path::rest) ->
        let results = node.SelectNodes(path)
        match results with
        | null -> tryPaths rest node
        | list -> list |> Seq.toList
    | [] ->
        failwith "no paths found"

let rec tryPath path (node: HtmlNode) =
    tryPaths [path] node

type ApartmentInfo = {
    Name: string
    Address: string
    YearBuilt: string option
}

let parseApartmentListing (doc: HtmlNode) =
    doc
    |> tryPath "//div[@class='placardContainer']//article//section/a[1]" 
    |> List.map (attr "href")
    |> List.map (fun (s: string) -> s.Replace("http://www.apartments.com/", ""))

let parseApartmentPage (doc: HtmlNode) =
    {
        Name = doc |> tryPaths ["//div[@class='propertyName']/span"; "//div[@class='propertyName']"] |> List.head |> innerText
        Address = doc |> tryPaths ["//div[@class='propertyAddress']/span"] |> List.map innerText |> String.concat " "
        YearBuilt = doc |> tryPaths ["//div[@class='specList propertyFeatures']//li"] |> List.map innerText |> List.tryFind (fun (x: string) -> x.StartsWith("Built"))
    }

let createApartmentListingRequest baseUrl city fragment =
    createRequest Get (sprintf "%s/%s/%s" baseUrl city fragment)

let createApartmentRecordRequest baseUrl fragment =
    createRequest Get (sprintf "%s/%s" baseUrl fragment)

type SuccessFail<'a> =
    | Success of 'a
    | Fail of exn list

type ParseStatus<'a> = 
    | ParseFail of exn list
    | ParseSuccess of 'a

type ScrapeResult<'a> =
    | FetchFail of exn list
    | FetchSuccess of string * ParseStatus<'a>

type RunRowStatus<'a> =
    | NotStarted
    | Waiting
    | Done of ScrapeResult<'a>

type RunRow<'a> = { Fragment: string; Status: RunRowStatus<'a> }
type RunState<'a, 'b> = { Results: RunRow<'a> list; OverallResult: 'b option }

let repeatOnFailure maxTries a =
    let rec inner n acc =
        async {
            if n < maxTries then
                let! result = a
                match result with 
                | Success _ ->
                    return result
                | Fail exns ->
                    return! inner (n+1) (exns::acc)
            else 
                return Fail (acc |> List.rev |> List.collect id)
        }
    inner 0 []

let wrapInTryCatch (f: 'a -> 'b) =
    fun x ->
        try
            f x |> Success
        with
        | exn -> Fail [ exn ]

let baseUrl = "http://www.apartments.com/"

let fetchHtmlForFragment (requestCreator: string -> Request) fragment =
    async {
        try
            let! html = 
                fragment
                |> requestCreator
                |> getResponseBodyAsync

            return Success html
        with
        | ex -> 
            return Fail [ ex ]
    }

let doRun (requestCreator: string -> Request) (parser: HtmlNode -> 'a) (reduce: 'a list -> 'b) (fragments: string list) =
    let rec inner runState =
        asyncSeq {
            let nextRowIdx = runState.Results |> List.tryFindIndex (fun x -> x.Status = NotStarted)
            match nextRowIdx with
            | Some idx ->
                let nextRow = List.nth runState.Results idx

                yield { runState with Results = runState.Results |> List.replaceAt idx { nextRow with Status = Waiting } }

                let! fetchResult = fetchHtmlForFragment requestCreator nextRow.Fragment |> repeatOnFailure 3

                let scrapeResult = 
                    match fetchResult with
                    | Success html ->
                        let parseResult = ((createDoc >> parser) |> wrapInTryCatch) html
                        
                        let parseResult =
                            match parseResult with
                            | Success parsed ->
                                ParseSuccess(parsed)
                            | Fail exns ->
                                ParseFail(exns)
                        
                        FetchSuccess(html, parseResult)
                    | Fail exns ->
                        FetchFail(exns)
                    
                let run = { runState with Results = runState.Results |> List.replaceAt idx { nextRow with Status = Done(scrapeResult) } }
            
                yield run
                yield! inner run
            | None ->
                let overallResult = 
                    runState.Results
                    |> List.map (fun r -> r.Status)
                    |> List.choose (function | Done (FetchSuccess(html, ParseSuccess(parsed))) -> Some parsed | _ -> None)
                    |> reduce
                yield { runState with OverallResult = Some overallResult }
        }

    asyncSeq {
        let initialRunState = { Results = fragments |> List.map (fun f -> { Fragment = f; Status = NotStarted }); OverallResult = None }
        yield initialRunState
        yield! inner initialRunState    
    }

let doListingRun city fragments =
    doRun (createApartmentListingRequest baseUrl city) parseApartmentListing (List.collect id >> Seq.distinct >> Seq.toList) fragments

let doRecordRun fragments =
    doRun (createApartmentRecordRequest baseUrl) parseApartmentPage (id) fragments
