@testset "parse_arguments tests" begin
    @testset "argument parsing with defaults" begin
        # Mock command line arguments with only required arguments
        test_args = [
            "--json-input", "path/to/json",
            "--output-dir", "/path/to/output",
        ]

        old_args = copy(ARGS)
        try
            empty!(ARGS)
            append!(ARGS, test_args)

            parsed = parse_arguments()

            # Test required arguments
            @test parsed["json-input"] == "path/to/json"
            @test parsed["output-dir"] == "/path/to/output"

            # Test key default values
            @test parsed["n-ahead"] == 8
            @test parsed["transformation"] == "boxcox"

        finally
            empty!(ARGS)
            append!(ARGS, old_args)
        end
    end
end
