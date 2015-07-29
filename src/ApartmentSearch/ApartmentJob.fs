module ApartmentJob

open HtmlAgilityPack
open Parsing
open HttpClient
open Util.HtmlAgility
open Scraping

let scrapeListingConfig =
    {
        RequestCreator = fun data -> createRequest Get (sprintf "http://www.apartments.com/durham-nc/%d" (data |> asInt))
        Parser = fun doc ->
            doc
            |> tryPath "//div[@class='placardContainer']//article//section/a[1]"
            |> List.map (attr "href")
            |> List.map (fun (s: string) -> s.Replace("http://www.apartments.com/", ""))
            |> List.map Data.String
            |> Data.List
        Reduce = List.map asList >> List.collect id >> Seq.distinct >> Seq.toList >> Data.List
    }

let scrapeRecordConfig =
    {
        RequestCreator = fun data -> createRequest Get (sprintf "http://www.apartments.com/%s" (data |> asString))
        Parser = fun doc ->
            [
                "Name", doc |> tryPaths ["//div[@class='propertyName']/span[1]"; "//div[@class='propertyName'][1]"] |> List.head |> innerText |> Data.String
                "Address", doc |> tryPaths ["//div[@class='propertyAddress']/span"] |> List.map innerText |> String.concat " " |> Data.String
                "YearBuilt", doc |> tryPaths ["//div[@class='specList propertyFeatures']//li"] |> List.map innerText |> List.tryFind (fun (x: string) -> x.StartsWith("Built")) |> Option.map Data.String |> Data.Option
            ] |> Map.ofList |> Data.Record
        Reduce = Data.List
    }

let jobConfig =
    {
        InitialInput = [1..2] |> List.map Data.Int
        Steps = 
            [
                scrapeListingConfig
                scrapeRecordConfig
            ]
    }
