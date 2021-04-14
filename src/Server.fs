open Saturn
open Giraffe
open Hobbes.Calculator.Services.Data
open Hobbes.Web
open Hobbes.Web.Routing
open Hobbes.Helpers.Environment
open Hobbes.Messaging.Broker
open Hobbes.Messaging
open Hobbes.Web.RawdataTypes

let private port = 8085

let private appRouter = router {
    not_found_handler (setStatusCode 404 >=> text "The requested ressource does not exist")
    fetch <@ listConfigurations @>
    fetch <@ ping @>
    withArg <@ configuration @>
    withArg <@ transformation @>
    withArg <@ sources @>
    fetch <@ collectors @>
    withBody <@ storeConfiguration @>
    withBody <@ storeTransformation @>
} 

let private app = application {
    url "http://0.0.0.0:8085/"
    use_router appRouter
    memory_cache
    use_gzip
}
let peek cacheKey =
    match Http.get (cacheKey |> Http.UniformDataService.Read |> Http.UniformData) Cache.CacheRecord.OfJson with
    Http.Error _ -> 
        false
    | Http.Success _ -> 
        true

type DependingTransformationList = FSharp.Data.JsonProvider<"""[
    {
        "_id" : "lkjlkj",
        "lines" : ["lkjlkj", "lkjlkj","o9uulkj"]
    }
]""">
let mutable time = System.DateTime.MinValue
let mutable dependencies : Map<string,seq<Transformation>> = Map.empty
let mutable readyForFormat : bool = false
let mutable merges : Map<string,Config.Root> = Map.empty
let mutable joins : Map<string,Config.Root> = Map.empty
let dependingTransformations (cacheKey : string) =
    assert(not(cacheKey.EndsWith(":") || System.String.IsNullOrWhiteSpace cacheKey))
#if DEBUG    
    let isCacheStale = true
#else
    let isCacheStale = (dependencies.Count = 0 || time < System.DateTime.Now.AddHours -1.)
#endif    
    if isCacheStale then
        time <- System.DateTime.Now
        readyForFormat <- false
        dependencies <-
            configurations.List()
            |> Seq.collect(fun configuration ->
                let transformations = 
                    configuration.Transformations
                    |> Array.map(fun transformationName ->
                        match transformations.TryGet transformationName with
                        None -> 
                            Log.errorf  "Transformation (%s) not found" transformationName
                            None
                        | t -> t
                    ) |> Array.filter Option.isSome
                    |> Array.map Option.get
                    |> Array.toList

                readyForFormat <- 
                    let expected = configuration.Transformations
                                   |> String.concat ":"
                    let actual = (cacheKey.IndexOf ":") + 1
                                 |> cacheKey.Substring
                    expected = actual || readyForFormat

                match transformations with
                [] -> []
                | h::tail ->
                    tail
                    |> List.fold(fun (lst : (string * Transformation) list) t ->
                        let prevKey, prevT = lst |> List.head
                        (prevKey + ":" + prevT.Name,t) :: lst
                    ) [keyFromSource configuration.Source,h]
            ) |> Seq.groupBy fst
            |> Seq.map(fun (key,deps) ->
                key,
                    deps 
                    |> Seq.map snd 
                    |> Seq.distinctBy(fun t -> t.Name)
            ) |> Map.ofSeq
        merges <-
            configurations.List()
            |> Seq.filter(fun configuration ->
                configuration.Source.Provider = "merge"
            ) |> Seq.collect(fun configuration ->
                configuration.Source.Datasets
                |> Array.map(fun ds -> ds,configuration)
            ) |> Map.ofSeq
        joins  <-
            configurations.List()
            |> Seq.filter(fun configuration ->
                configuration.Source.Provider = "join"
            ) |> Seq.collect(fun configuration ->
                [
                    configuration.Source.Left.Value,configuration
                    configuration.Source.Right.Value,configuration
                ]
            ) |> Map.ofSeq
    match dependencies |> Map.tryFind cacheKey with
    None -> 
        Log.debugf "No dependencies found for key (%s)" cacheKey
        Seq.empty, true
    | Some dependencies ->
        let shouldFormat = 
               dependencies |> Seq.isEmpty || readyForFormat
        dependencies, shouldFormat
let handleMerges cacheKey = 
    match merges |> Map.tryFind cacheKey with
    None -> ()
    | Some configuration ->
        let allUpdated = 
            configuration.Source.Datasets
            |> Array.forall(peek)

        if allUpdated then
            {
                CacheKey = configuration |> keyFromConfig
                Datasets = configuration.Source.Datasets
            } |> Merge
            |> Broker.Calculation

let handleJoins cacheKey = 
    match joins |> Map.tryFind cacheKey with
    None -> ()
    | Some configuration ->
        {
            CacheKey = configuration |> keyFromConfig
            Left = configuration.Source.Left.Value
            Right = configuration.Source.Right.Value
            Field = configuration.Source.Field.Value
        } |> Join
        |> Broker.Calculation
let getDependingTransformations (cacheMsg : CacheMessage) = 
    try
         match cacheMsg with
         CacheMessage.Empty -> Success
         | Updated cacheKey -> 
            let depending, format = dependingTransformations cacheKey
            if format then
                Log.debugf "Final json for %s" cacheKey
                {
                   Format = Json
                   CacheKey = cacheKey
                }
                |> Format
                |> Broker.Calculation
            if Seq.isEmpty depending |> not then
                depending
                |> Seq.iter(fun transformation ->    
                    {
                        Transformation = 
                            {
                                Name = transformation.Name
                                Statements = transformation.Statements
                            }
                        DependsOn = cacheKey
                    }
                    |> Transform
                    |> Broker.Calculation
                )
            else
                Log.debugf "No dependencies so expecting final json for %s" cacheKey
                {
                   Format = Json
                   CacheKey = cacheKey
                }
                |> Format
                |> Broker.Calculation
            handleMerges cacheKey
            handleJoins cacheKey
            Success
    with e ->
        Log.excf e "Failed to perform calculation."
        Excep e

[
   "configurations"
   "transformations"
] |> Database.initDatabases

async {    
    do! awaitQueue()
    Broker.Cache getDependingTransformations
} |> Async.Start

run app