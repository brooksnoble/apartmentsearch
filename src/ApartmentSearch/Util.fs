module Util

module List =
    let chunk n list =
        list
        |> List.fold (
            fun (cur, acc) x -> 
                if List.length cur = n then ([ x ], (cur::acc)) 
                else ((x::cur), acc)) ([], [])
        |> (fun (cur, acc) -> (cur::acc))
        |> List.map List.rev
        |> List.rev

    let rec replaceAt idx newElem list =
        match list with
        | [] -> []
        | (head::tail) ->
            match idx with
            | 0 -> (newElem::tail)
            | n -> (head::(replaceAt (idx-1) newElem tail))


module Expr =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter
    open System.Linq.Expressions
    open System

    let toLinq (expr : Expr<'a -> 'b>) =
        let linq = expr |> QuotationToExpression
        let call = linq :?> MethodCallExpression
        let lambda = call.Arguments.[0] :?> LambdaExpression
        Expression.Lambda<Func<'a, 'b>>(lambda.Body, lambda.Parameters) 

module Async =
    open System.Threading.Tasks

    let inline awaitPlainTask (task: Task) = 
        // rethrow exception from preceding task if it faulted
        let continuation (t : Task) : unit =
            match t.IsFaulted with
            | true -> raise t.Exception
            | arg -> ()
        task.ContinueWith continuation |> Async.AwaitTask
 
    let map f (a: Async<'a>) =
        async { let! x = a
                return f x }

    let InSeries (asyncs: Async<'a> list) =
        let rec loop results asyncs = 
            async {
                match asyncs with
                | (head::rest) ->
                    let! result = head
                    return! loop (result::results) rest
                | [] ->
                    return results
            }
        
        loop [] asyncs 
        |> (fun a -> async { let! x = a
                             return List.rev x })

    let InBatches batchSize pauseBetweenMs (asyncs: Async<'a> list) =
        asyncs
        |> List.chunk batchSize
        |> List.map Async.Parallel
        |> List.map (map Array.toList)
        |> List.map (fun x -> [x; Async.Sleep pauseBetweenMs |> map (fun () -> [])])
        |> List.collect id
        |> InSeries
        |> map (List.collect id)


module HtmlAgility =
    open HtmlAgilityPack

    let createDoc html =
        let doc = new HtmlDocument()
        doc.LoadHtml html
        doc.DocumentNode

    let inline attr name (node : HtmlNode) =
        node.GetAttributeValue(name, "")

    let inline innerText (node : HtmlNode) =
        node.InnerText


let env (key: string) =
    match System.Environment.GetEnvironmentVariable key with
    | null -> 
        match System.Configuration.ConfigurationManager.AppSettings.[key] with
        | null -> failwith ("Required environment variable not found: " + key)
        | x -> x
    | x -> x
