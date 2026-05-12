using Test, EpiAutoGP
using JSON3
using ArgParse
using Dates
using DBInterface
using DuckDB
using CSV
using DataFramesMeta
using Random
using Statistics
using NowcastAutoGP

# Run all tests in the test directory
include("test_parse_arguments.jl")
include("test_input.jl")
include("test_modelling.jl")
include("test_output.jl")

println("All EpiAutoGP tests completed!")
