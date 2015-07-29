module Scraping

open HttpClient
open HtmlAgilityPack
open Util
open Util.HtmlAgility
open FSharp.Control

[<AutoOpen>]
module Data = 
    type Data =
        | Int of int
        | String of string
        | Record of Map<string, Data>
        | Option of Data option
        | List of List<Data>

    let asInt = function | Int i -> i | _ -> failwith "unexpected Data type"
    let asString = function | String i -> i | _ -> failwith "unexpected Data type"
    let asList = function | List list -> list | _ -> failwith "unexpected Data type"

[<AutoOpen>]
module Config = 
    type StepConfig<'TInput, 'TParseResult, 'TOutput> = {
        RequestCreator: 'TInput -> Request
        Parser: HtmlNode -> 'TParseResult
        Reduce: 'TParseResult list -> 'TOutput
    }

    type JobConfig = {
        InitialInput: Data list
        Steps: StepConfig<Data, Data, Data> list
    }

[<AutoOpen>]
module Results = 
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

    type StepRow<'TInput, 'TParseResult> = { Fragment: 'TInput; Status: RunRowStatus<'TParseResult> }
    type RunningStepState<'TInput, 'TParseResult, 'TOutput> = { Results: StepRow<'TInput, 'TParseResult> list; OverallResult: 'TOutput option }

    type StepState =
        | WaitingForInput
        | Running of RunningStepState<Data, Data, Data>

    type JobState = {
        Steps: StepState array
    }

[<AutoOpen>]
module Helpers = 
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

let doJobStep (config: StepConfig<'TInput, 'TParseResult, 'TOutput>) (fragments: 'TInput list) =
    let fetchHtmlForFragment requestCreator fragment =
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

    let rec inner runState =
        asyncSeq {
            let nextRowIdx = runState.Results |> List.tryFindIndex (fun x -> x.Status = NotStarted)
            match nextRowIdx with
            | Some idx ->
                let nextRow = List.nth runState.Results idx

                yield { runState with Results = runState.Results |> List.replaceAt idx { nextRow with Status = Waiting } }

                let! fetchResult = fetchHtmlForFragment config.RequestCreator nextRow.Fragment |> repeatOnFailure 3

                let scrapeResult = 
                    match fetchResult with
                    | Success html ->
                        let parseResult = ((createDoc >> config.Parser) |> wrapInTryCatch) html
                        
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
                    |> config.Reduce
                yield { runState with OverallResult = Some overallResult }
        }

    asyncSeq {
        let initialRunState = { Results = fragments |> List.map (fun f -> { Fragment = f; Status = NotStarted }); OverallResult = None }
        yield initialRunState
        yield! inner initialRunState    
    }

let replace idx newValue array =
    array
    |> Array.mapi (fun i x -> if i = idx then newValue else x)


let doJob (config: JobConfig) =
    let rec inner input steps idx (jobState: JobState) =
        asyncSeq {
            match steps with
            | (nextStep::rest) ->
                let last = ref None
                for stepState in doJobStep nextStep input do
                    let jobState = { jobState with Steps = jobState.Steps |> replace idx (Running(stepState)) }
                    yield jobState
                    last := Some (stepState, jobState)
                
                let stepState, jobState = last.Value.Value

                yield! inner (stepState.OverallResult.Value |> asList) rest (idx+1) jobState
            | [] ->
                ()
        }

    asyncSeq {
        let initialJobState = { Steps = config.Steps |> List.toArray |> Array.map (fun _ -> WaitingForInput) }
        yield initialJobState
        yield! inner config.InitialInput config.Steps 0 initialJobState
    }