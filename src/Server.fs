open Saturn
open Giraffe
open Hobbes.Calculator.Services.Data
open Hobbes.Web
open Hobbes.Web.Routing
open Hobbes.Helpers.Environment
open Hobbes.Messaging.Broker
open Hobbes.Messaging
open Hobbes.Web.RawdataTypes

let private port = env "port" "8085" |> int

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
    url (sprintf "http://0.0.0.0:%d/" port)
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
       
[
   "configurations"
   "transformations"
] |> Database.initDatabases

run app