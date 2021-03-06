

open DuckDB.NET.Data
open Expecto
open Expecto.Flip
open System.IO
open Dapper
open System

module Async =
    let map f computation = 
        async.Bind(computation, f >> async.Return)

    let zip c1 c2 =
        async{
            let! x = Async.StartChild c1
            let! y = Async.StartChild c2
            let! x' = x
            let! y' = y
            return x', y'
        }

type AsyncBuilder with
    member _.BindReturn(x: Async<'t>, f) = Async.map f x
    member _.MergeSources(x: Async<'t>, y: Async<'u>) = Async.zip x y

type IDatabase =
    abstract OpenConnection: unit -> Async<DuckDBConnection>

type IDb = abstract Database: IDatabase

let initialiseDb (env: #IDb) = async{
    let dummyRecords =
        seq{for i in 1 .. 100000 do
            $"sensor_{i}, {i}"
        }

    let fileName = $"{dummyRecords.GetHashCode()}.csv"
    File.WriteAllLines(fileName, dummyRecords)

    let createSql = "create table test_table(col1 text, col2 integer);"
    let! conn = env.Database.OpenConnection()
    conn.Execute(createSql) |> ignore

    let insertSql = $"copy test_table from '{fileName}' (auto_detect true);"
    conn.Execute(insertSql) |> ignore
    File.Delete(fileName)
}
    
type DbReturn<'t> = {query_value: 't}
let selectMin (env: #IDb) = async{
    use! conn = env.Database.OpenConnection()
    let x = conn.Query<DbReturn<int>>("select min(col2) query_value from test_table") |> Seq.exactlyOne
    return x.query_value
}
    
let selectMax (env: #IDb) = async{
    use! conn = env.Database.OpenConnection()
    let x = conn.Query<DbReturn<int>>("select max(col2) query_value from test_table") |> Seq.exactlyOne
    return x.query_value
}
    
let selectAvg (env: #IDb) = async{
    use! conn = env.Database.OpenConnection()
    let x = conn.Query<DbReturn<float>>("select avg(col2) query_value from test_table") |> Seq.exactlyOne
    return x.query_value |> int
}

let getSum1 env = async{
    //sequentially run the queries
    let! minVal = selectMin env
    let! maxVal = selectMax env
    let! avgVal = selectAvg env

    return minVal + maxVal + avgVal
}

let getSum2 env = async{
    //concurrently run the queries
    let! minVal = selectMin env
    and! maxVal = selectMax env
    and! avgVal = selectAvg env

    return minVal + maxVal + avgVal
}

let test1 env = testAsync "Sequential Connection Sequential Query" {
    let! sum = getSum1 env
    sum |> Expect.equal "" 150001
}


let test2 (env: #IDb) = testAsync "Sequential Connection Concurrent Query" {
    let! sum = getSum2 env
    sum |> Expect.equal "" 150001
}

let test3 (env: #IDb) = testAsync "Sequential Connection Concurrent Query2" {
    let! sum = getSum2 env
    sum |> Expect.equal "" 150001
}

let allTests env = 
    testList "" 
        [
            //test1 env
            //test2 env
            test3 env
        ]

[<EntryPoint>]
let main argv =
    let dbName = "test.db"
    if File.Exists(dbName) then
        File.Delete(dbName)
    //implement the database dependency interface
    let database = {
        new IDatabase with
            member this.OpenConnection() = async{
                return new DuckDBConnection($"Data Source={dbName}")
            }
    }
    
    let env = {
        new IDb with
            member this.Database = database
    }

    initialiseDb env |> Async.RunSynchronously
    runTestsWithCLIArgs [] argv (allTests env)