namespace Hobbes.Calculator.Services

open Hobbes.Web.Routing
open Hobbes.Web
open Hobbes.Web.Http
open FSharp.Data
open Hobbes.Web.RawdataTypes
open Thoth.Json.Net

[<RouteArea ("/data", false)>]
module Data =
    
    let configurations = Database.Database("configurations", Config.Parse, Log.loggerInstance)
    let transformations = Database.Database("transformations", Newtonsoft.Json.JsonConvert.DeserializeObject<Transformation>, Log.loggerInstance)
    
    
    let private listConfigurations () = 
        configurations.List()
        |> Seq.filter(fun config ->
            config.JsonValue.Properties() 
            |> Array.tryFind(fun (name,_) -> name = "source") 
            |> Option.isSome
        )

    [<Get ("/configurations")>]
    let allConfigurations() = 
        200,(",",listConfigurations()
                 |> Seq.map(fun c ->
                    c.JsonValue.ToString()
                 )
            ) |> System.String.Join
            |> sprintf "[%s]"

    [<Get ("/configuration/%s")>]
    let configuration (configurationName : string) =
        match configurations.TryGet configurationName with
        None -> 404, sprintf "Configuration (%s) not found" configurationName
        | Some c -> 200, c.JsonValue.ToString()

    [<Get ("/transformation/%s")>]
    let transformation (transformationName : string) =
        match transformations.TryGet transformationName with
        None -> 404, sprintf "Transformation (%s) not found" transformationName
        | Some transformation -> 200, transformation |> Newtonsoft.Json.JsonConvert.SerializeObject

    [<Post ("/configuration", true)>]
    let storeConfiguration (configuration : string) =
        let conf = Config.Parse configuration
        let hasValue d = System.String.IsNullOrWhiteSpace d |> not
        
        assert(hasValue conf.Id)
        assert(hasValue conf.Source.Provider)
        assert(conf.Source.Provider <> "rest" || conf.Source.Urls.Length > 0)
        assert(conf.Source.Provider <> "odata" || hasValue(conf.Source.Url.Value))

        200,configurations.InsertOrUpdate configuration

    [<Post ("/transformation", true)>]
    let storeTransformation (transformation : string) =
        let trans = 
            try
                Newtonsoft.Json.JsonConvert.DeserializeObject<Transformation> transformation
            with e ->
               Log.excf e "Failed to deserialize %s" transformation
               reraise()

        assert(System.String.IsNullOrWhiteSpace(trans.Name) |> not)
        assert(trans.Statements |> List.isEmpty |> not)

        200,transformations.InsertOrUpdate transformation

    [<Get "/ping">]
    let ping () =
        200, "pong - Configurations"