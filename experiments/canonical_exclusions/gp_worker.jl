# Fit JSON-line requests in one process, resetting the RNG for every fit.
using Random
include(joinpath(@__DIR__, "../../src/cfa/stf/routine/nowcastautogp/fit_nowcastautogp.jl"))

for line in eachline(stdin)
    request = JSON3.read(line)
    response = Dict{String, Any}("ok" => true)
    open(String(request.log), "w") do io
        redirect_stdout(io) do
            redirect_stderr(io) do
                try
                    Random.seed!(request.seed)
                    args = parse_arguments(String.(request.arguments))
                    input = read_and_validate_data(args["json-input"])
                    results = forecast_with_nowcastautogp(input, args)
                    create_forecast_output(input, results, args["output-dir"]; save_output = true)
                catch error
                    showerror(io, error, catch_backtrace())
                    response["ok"] = false
                    response["error"] = sprint(showerror, error)
                end
            end
        end
    end
    println(JSON3.write(response))
    flush(stdout)
    GC.gc()
end
