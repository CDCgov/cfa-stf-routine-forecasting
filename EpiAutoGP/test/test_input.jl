
function create_sample_input(output_path::String; n_weeks::Int=30,
    pathogen::String="COVID-19", location::String="CA")
    start_date = Date("2024-01-01")
    dates = [start_date + Week(i) for i in 0:(n_weeks-1)]
    reports = [rand(20:100) + 10 * sin(2π * i / 7) + rand() * 5 for i in 1:n_weeks]  # Weekly pattern with noise

    forecast_date = dates[end]
    nowcast_dates = dates[max(1, end - 2):end]  # Last 3 days
    nowcast_reports = [[reports[max(1, end - 2)+j-1] + rand(-5:5) for j in 1:3]
                       for _ in 1:10]  # 10 realizations, each with 3 values

    input_data = EpiAutoGPInput(
        dates, reports, pathogen, location, "nhsn", "epiweekly", "observed",
        forecast_date, nowcast_dates, nowcast_reports
    )

    # Write to JSON file
    open(output_path, "w") do f
        JSON3.write(f, input_data)
    end

    return input_data
end

@testset "EpiAutoGPInput Tests" begin
    @testset "Construction and Serialization" begin
        # Test valid construction
        dates = [Date("2024-01-01"), Date("2024-01-02"), Date("2024-01-03")]
        reports = [45.0, 52.0, 38.0]
        nowcast_reports = [[50.0, 36.0], [52.0, 38.0]]

        input_data = EpiAutoGPInput(
            dates, reports, "COVID-19", "CA", "nhsn", "daily", "observed",
            Date("2024-01-03"), dates[2:3], nowcast_reports
        )

        @test input_data.dates == dates
        @test input_data.pathogen == "COVID-19"
        @test length(input_data.nowcast_reports) == 2

        # Test JSON round-trip
        json_string = JSON3.write(input_data)
        parsed = JSON3.read(json_string, EpiAutoGPInput)
        @test parsed.dates == input_data.dates
        @test parsed.pathogen == input_data.pathogen
    end

    @testset "Data Validation - Valid Cases" begin
        valid_data = EpiAutoGPInput(
            [Date("2024-01-01"), Date("2024-01-02")],
            [45.0, 52.0],
            "COVID-19", "CA", "nhsn", "daily", "observed",
            Date("2024-01-02"),
            [Date("2024-01-02")],
            [[50.0], [52.0]]
        )
        @test validate_input(valid_data) == true

        # Test without nowcasts
        no_nowcast = EpiAutoGPInput(
            [Date("2024-01-01")], [10.0], "COVID-19", "TX", "nhsn", "daily", "observed",
            Date("2024-01-01"), Date[], Vector{Real}[]
        )
        @test validate_input(no_nowcast) == true
    end

    @testset "Data Validation - Invalid Cases" begin
        # Mismatched lengths
        @test_throws ArgumentError validate_input(EpiAutoGPInput(
            [Date("2024-01-01"), Date("2024-01-02")], [45.0],
            "COVID-19", "CA", "nhsn", "daily", "observed", Date("2024-01-01"), Date[], Vector{Real}[]
        ))

        # Empty data
        @test_throws ArgumentError validate_input(EpiAutoGPInput(
            Date[], Real[], "COVID-19", "CA", "nhsn", "daily", "observed", Date("2024-01-01"), Date[], Vector{Real}[]
        ))

        # Unsorted dates
        @test_throws ArgumentError validate_input(EpiAutoGPInput(
            [Date("2024-01-02"), Date("2024-01-01")], [45.0, 52.0],
            "COVID-19", "CA", "nhsn", "daily", "observed", Date("2024-01-02"), Date[], Vector{Real}[]
        ))

        # Invalid nowcast structure
        @test_throws ArgumentError validate_input(EpiAutoGPInput(
            [Date("2024-01-01")], [45.0], "COVID-19", "CA", "nhsn", "daily", "observed", Date("2024-01-01"),
            [Date("2024-01-01")], [[50.0, 55.0]]  # Wrong length
        ))

        # Negative values
        @test_throws ArgumentError validate_input(EpiAutoGPInput(
            [Date("2024-01-01")], [-5.0], "COVID-19", "CA", "nhsn", "daily", "observed", Date("2024-01-01"), Date[], Vector{Real}[]
        ))
    end

    @testset "File I/O" begin
        tmpfile = tempname() * ".json"
        try
            test_data = Dict(
                "dates" => ["2024-01-01", "2024-01-02"],
                "reports" => [45.0, 52.0],
                "pathogen" => "COVID-19",
                "location" => "CA",
                "target" => "nhsn",
                "frequency" => "daily",
                "ed_visit_type" => "observed",
                "forecast_date" => "2024-01-02",
                "nowcast_dates" => ["2024-01-02"],
                "nowcast_reports" => [[50.0], [55.0]]
            )

            open(tmpfile, "w") do f
                JSON3.write(f, test_data)
            end

            loaded = read_and_validate_data(tmpfile)
            @test loaded.pathogen == "COVID-19"
            @test length(loaded.dates) == 2
        finally
            isfile(tmpfile) && rm(tmpfile)
        end
    end

    @testset "Sample Creation" begin
        tmpdir = mktempdir()
        try
            json_path = joinpath(tmpdir, "sample.json")
            sample = create_sample_input(json_path; n_weeks=14, pathogen="Influenza")

            @test validate_input(sample) == true
            @test sample.pathogen == "Influenza"
            @test length(sample.dates) == 14
            @test isfile(json_path)

            loaded = read_and_validate_data(json_path)
            @test loaded.pathogen == sample.pathogen
        finally
            rm(tmpdir, recursive=true)
        end
    end
end
