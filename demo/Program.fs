

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
let selectMin (conn: DuckDBConnection) = async{
    let x = conn.Query<DbReturn<int>>("select min(col2) query_value from test_table") |> Seq.exactlyOne
    return x.query_value
}
    

let selectMax (conn: DuckDBConnection) = async{
    let x = conn.Query<DbReturn<int>>("select max(col2) query_value from test_table") |> Seq.exactlyOne
    return x.query_value
}
    

let selectAvg (conn: DuckDBConnection) = async{
    let x = conn.Query<DbReturn<float>>("select avg(col2) query_value from test_table") |> Seq.exactlyOne
    return x.query_value |> int
}

let getSum1 (env: #IDb) = async{
    //sequentially open the connections
    let! con1 = env.Database.OpenConnection()
    let! con2 = env.Database.OpenConnection()
    let! con3 = env.Database.OpenConnection()

    //sequentially run the queries
    let! minVal = selectMin con1
    let! maxVal = selectMax con2
    let! avgVal = selectAvg con3

    return minVal + maxVal + avgVal
}

let getSum2 (env: #IDb) = async{
    //sequentially open the connections
    let! con1 = env.Database.OpenConnection()
    let! con2 = env.Database.OpenConnection()
    let! con3 = env.Database.OpenConnection()

    //concurrently run the queries
    let! minVal = selectMin con1
    and! maxVal = selectMax con2
    and! avgVal = selectAvg con3

    return minVal + maxVal + avgVal
}

let getSum3 (env: #IDb) = async{
    //concurrently open the connections
    let! con1 = env.Database.OpenConnection()
    and! con2 = env.Database.OpenConnection()
    and! con3 = env.Database.OpenConnection()

    //concurrently run the queries
    let! minVal = selectMin con1
    and! maxVal = selectMax con2
    and! avgVal = selectAvg con3

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


let test3 (env: #IDb) = testAsync "Concurrent Connection Concurrent Query" {
    let! sum = getSum3 env
    sum |> Expect.equal "" 150001
}


let allTests env = 
    testList "" 
        [
            test1 env
            test2 env
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