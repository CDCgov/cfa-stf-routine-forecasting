
@testset "Modelling Functions Tests" begin

    # Helper function to create test input data
    function create_test_input(; include_nowcasts=true)
        dates = [Date(2024, 1, 1) + Week(i - 1) for i in 1:10]
        reports = Float64[1000, 1100, 1050, 1150, 1200, 1250, 1300, 1350, 1400, 1450]

        if include_nowcasts
            nowcast_dates = dates[(end-1):end]
            nowcast_reports = [Float64[1350, 1400], Float64[1400, 1450]]
        else
            nowcast_dates = Date[]
            nowcast_reports = Vector{Float64}[]
        end

        return EpiAutoGPInput(
            dates, reports, "COVID-19", "US", "nhsn", "epiweekly", "observed",
            dates[end], nowcast_dates, nowcast_reports
        )
    end

    @testset "prepare_for_modelling" begin
        # Test with nowcasts
        input_with_nowcasts = create_test_input(include_nowcasts=true)
        result = prepare_for_modelling(input_with_nowcasts, "boxcox", 4, 100)

        @test haskey(result, :stable_data_dates)
        @test haskey(result, :transformation)
        @test length(result.stable_data_dates) == 8  # 10 total - 2 nowcast
        @test length(result.forecast_dates) == 5  # 0, 1, 2, 3, 4 weeks ahead
        @test result.forecast_dates[1] == input_with_nowcasts.forecast_date  # Starts at week 0
        @test ~isnothing(result.nowcast_data)  # Should have nowcast data

        # Test without nowcasts
        input_no_nowcasts = create_test_input(include_nowcasts=false)
        result_no_nowcast = prepare_for_modelling(input_no_nowcasts, "boxcox", 4, 100)

        @test haskey(result_no_nowcast, :nowcast_data)
        @test isnothing(result_no_nowcast.nowcast_data)  # Should be nothing when no nowcasts
        @test length(result_no_nowcast.stable_data_dates) == 10  # All data is stable
    end

    @testset "fit_base_model" begin
        input = create_test_input(include_nowcasts=true)
        prep_result = prepare_for_modelling(input, "positive", 2, 50)

        model = fit_base_model(
            prep_result.stable_data_dates, prep_result.stable_data_values;
            transformation=prep_result.transformation,
            n_particles=1, smc_data_proportion=0.5, n_mcmc=3, n_hmc=3
        )

        @test model !== nothing
    end

    @testset "forecast_with_epiautogp" begin
        input = create_test_input(include_nowcasts=false)

        forecast_dates,
        forecasts = forecast_with_epiautogp(input;
            n_ahead=2, n_forecasts=10,
            transformation_name="positive",
            n_particles=1, smc_data_proportion=0.5, n_mcmc=3, n_hmc=3
        )

        @test length(forecast_dates) == 3  # 0, 1, 2 weeks ahead
        @test size(forecasts, 1) == 3
        @test size(forecasts, 2) == 10
        @test all(forecasts .> 0)
    end

    @testset "forecast_with_epiautogp - daily frequency" begin
        # Create daily frequency test data
        dates = [Date(2024, 1, 1) + Day(i - 1) for i in 1:30]
        reports = Float64[100 + 10 * sin(i / 5) + 2 * i for i in 1:30]

        input = EpiAutoGPInput(
            dates, reports, "COVID-19", "US", "nhsn", "daily", "observed",
            dates[end], Date[], Vector{Float64}[]
        )

        forecast_dates,
        forecasts = forecast_with_epiautogp(input;
            n_ahead=7, n_forecasts=10,
            transformation_name="positive",
            n_particles=1, smc_data_proportion=0.5, n_mcmc=3, n_hmc=3
        )

        @test length(forecast_dates) == 8  # 0, 1, 2, ..., 7 days ahead
        @test size(forecasts, 1) == 8
        @test size(forecasts, 2) == 10
        @test all(forecasts .> 0)

        # Verify forecast dates increment by Day(1)
        @test forecast_dates[1] == dates[end]
        @test forecast_dates[2] == dates[end] + Day(1)
        @test forecast_dates[end] == dates[end] + Day(7)
    end
end
