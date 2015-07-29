module Parsing

open HtmlAgilityPack

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
