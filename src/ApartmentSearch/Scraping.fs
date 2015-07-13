module Scraping

open HttpClient
open HtmlAgilityPack
open Util
open Util.HtmlAgility

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
    YearBuilt: string
}

type ScrapeResult =
    | Success of ApartmentInfo
    | Failure of exn list

let parseListing (doc: HtmlNode) =
    doc
    |> tryPath "//div[@class='placardContainer']//article//a[1]" 
    |> List.map (attr "href")

let parseApartmentPage (doc: HtmlNode) =
    {
        Name = doc |> tryPaths ["//div[@class='propertyName']/span"; "//div[@class='propertyName']"] |> List.head |> innerText
        Address = doc |> tryPaths ["//div[@class='propertyAddress']/span"] |> List.map innerText |> String.concat " "
        YearBuilt = doc |> tryPaths ["//div[@class='specList propertyFeatures']//li"] |> List.head |> innerText
    }

let scrapeApartmentUrls city baseUrl page = 
    async {
        let! html = 
            createRequest Get (sprintf "%s/%s/%d" baseUrl city page)
            |> getResponseBodyAsync

        return 
            html 
            |> createDoc
            |> parseListing
    }

let scrapeApartmentInfo aptUrl = 
    async {
        try
            let! html = 
                createRequest Get aptUrl
                |> getResponseBodyAsync

            return 
                html 
                |> createDoc
                |> parseApartmentPage
                |> Success

        with
            | ex -> 
                return Failure [ex]
    }

let repeatOnFailure maxTries a =
    let rec inner n acc =
        async {
            if n < maxTries then
                let! result = a
                match result with 
                | Success _ ->
                    return result
                | Failure exns ->
                    return! inner (n+1) (exns::acc)
            else 
                return Failure (acc |> List.rev |> List.collect id)
        }
    inner 0 []

let baseUrl = "http://www.apartments.com/"

type ApartmentScrape = { Url: string; Result: ScrapeResult }

let doScrape city pageRange =
    pageRange
    |> List.map (scrapeApartmentUrls city baseUrl)
    |> Async.InBatches 2 3000
    |> Async.map
        (List.collect id
         >> List.map (fun url -> scrapeApartmentInfo url 
                                 |> repeatOnFailure 5
                                 |> Async.map (fun x -> { Url = url; Result = x }))
         >> Async.InBatches 3 500)
    |> (fun x -> async { let! a = x; 
                         return! a })
