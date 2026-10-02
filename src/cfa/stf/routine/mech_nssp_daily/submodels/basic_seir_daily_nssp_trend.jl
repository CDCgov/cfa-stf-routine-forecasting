# Count-scale SEIRS-family submodel
# Model definition and equations: see submodels/README.md.

using ConfigurableEpi
using AlgebraicEpiMech
using Catlab: dom
using Configurations: @option, from_dict
using Dates: dayofweek, value
import AlgebraicPetri: tnames

# Defaults used to initialize the NSSP latent ascertainment trend. The level, initial rate and
# calendar anchor come from reports/susceptible-reconstruction and are recorded in
# docs/parameter-provenance.md. The decline-rate prior is derived from the ED-visit annual factor
# `phi = 0.678`: `r = -log(phi) / 2`, with a 90% interval spanning no decline through the full
# observed decline. The floor and bound apply only when projecting the registered level from its
# reference date to the first model day; they do not constrain the filtered trend.
const ASCERTAINMENT_DECLINE_RATE_PRIOR_MEAN = 0.1943
const ASCERTAINMENT_DECLINE_RATE_PRIOR_SD = 0.1181
const DEFAULT_ASCERTAINMENT_DECLINE_RATE = ASCERTAINMENT_DECLINE_RATE_PRIOR_MEAN
const DEFAULT_ASCERTAINMENT_FLOOR_FRACTION = 0.2
const DEFAULT_ASCERTAINMENT_REFERENCE_DATE = "2024-07-01"
const DEFAULT_ASCERTAINMENT_RATE_BOUND = 1.0

const SEASONAL_KAPPA = 0.8
const PRODUCTION_SEASONALITY = SeasonalityConfig(
    mode = "indoor_activity", kappa = SEASONAL_KAPPA, fallback = "error",
)
const LEARNED_PARAMETERS = (
    :Rt_sigma_stat, :R0_baseline, ASCERTAINMENT_TREND_WANDER,
)

# ---------------------------------------------------------------------------
# Epi config — this submodel OWNS its schema (mean durations + Erlang stage counts +
# fixed params). Typed + validated via Configurations.jl; `Durations` is the shared
# primitive from the package. `parse_epi_config` turns the runner's raw model settings into this
# typed config; `build_model` receives the result on `ctx.epi`.
# ---------------------------------------------------------------------------

# Most of these are now inference start values rather than operating constants (the PF's
# particle-cloud centre), so they track their prior means. `Rt_mu`,
# `ascertainment` and `Rt_tau` are the genuinely fixed ones: μ_R = 1 makes `R0_baseline` the sole
# transmission level, ascertainment is not identifiable alongside it from one signal, and the
# latent's correlation time is asserted (see below) because the one-step filter likelihood pulls
# a learned memory toward zero rather than identifying it.
# `ascertainment` is the registered level at a fixed calendar date. It and the initial decline
# rate determine the latent trend's level on the first model day.
@option struct BasicSeirFixed
    R0_baseline::Float64 = 2.0
    # Infection -> observed COVID-coded ED visit; fixed because it is confounded with transmission.
    # Its derivation and coupling to `R0_baseline` and immunity are recorded in
    # docs/parameter-provenance.md and guarded by examples/parameter_model_check.jl.
    # Observations per infection at `ascertainment_reference_date`. The level itself is not
    # identifiable alongside the transmission level from one signal.
    ascertainment::Float64 = 0.005
    # Initial latent decline rate per year (`exp(-rate)` is the annual factor).
    ascertainment_decline_rate::Float64 = DEFAULT_ASCERTAINMENT_DECLINE_RATE
    # Retained in the reference-date-to-start-date projection to reproduce the backtested seed.
    ascertainment_floor_fraction::Float64 = DEFAULT_ASCERTAINMENT_FLOOR_FRACTION
    # Calendar date (ISO string) at which `ascertainment` is the registered level.
    ascertainment_reference_date::String = DEFAULT_ASCERTAINMENT_REFERENCE_DATE
    # Bound used only when projecting the registered level to the first model day.
    ascertainment_rate_bound::Float64 = DEFAULT_ASCERTAINMENT_RATE_BOUND
    Rt_mu::Float64 = 1.0
    # The latent is a correction to the EXPECTED SEASONALITY: `log chi(t) = log f(t) + AR(t)`, with
    # `f` the forcing below and `AR` this OU process. Its correlation time is the timescale on which
    # a year's forcing departs from the climatology (an early or late season, a holiday surge, an
    # off-season variant wave: one to three months), and its stationary log-space sd is how far
    # the fixed indoor-activity forcing is trusted at any instant. The pair sits on the wave-generation
    # bound `Λ = σ_stat·τ/(T_g·A) ≈ 0.27` (docs/parameter-provenance.md §2.4 and ADR-0003): a
    # longer memory is available only by trusting the forcing more tightly.
    Rt_tau::Float64 = 30.0
    Rt_sigma_stat::Float64 = 0.05
    phi::Float64 = 140.0
end

# The daily model with ascertainment represented by a latent integrated Brownian motion. The
# configuration intentionally carries no trend-specific block: the wander prior belongs under
# `[priors.ascertainment_trend_wander]`, and Liu-West forgetting belongs under
# `[hyper.liu_west.forgetting_memory_days]`.
@option "basic_seir_daily_nssp_trend" struct BasicSeirDailyNSSPTrendEpiConfig
    model_type::String = "SEIRS"
    n_E_stages::Int = 1
    n_I_stages::Int = 1
    n_obs_stages::Int = 2
    start_at_peak::Bool = false
    durations_days::Durations = Durations()
    fixed::BasicSeirFixed = BasicSeirFixed()
end

function parse_epi_config(raw::AbstractDict, ::Val{:basic_seir_daily_nssp_trend})
    cfg = from_dict(BasicSeirDailyNSSPTrendEpiConfig, raw)
    validate_ascertainment(cfg.fixed)
    return cfg
end

# Model-science priors. `Rt` and the ascertainment decline rate initialize filtered states;
# `Rt_sigma_stat`, `R0_baseline` and `ascertainment_trend_wander` are learned by default.
default_priors() = Dict{String, PriorSpec}(
    # AR1 Rt initial state, at the process's OWN STATIONARY spread: an unobserved stationary
    # process has its stationary law as its marginal at t = 0, and `sqrt(exp(s^2) - 1)` is the
    # constrained-space sd whose log-chart variance is exactly `Rt_sigma_stat^2` — see
    # docs/parameter-provenance.md 2.4.
    "Rt" => PriorSpec(mean = 1.0, std = 0.05003126628214686),
    # AR1 Rt STATIONARY sd (log space): how far the seasonal forcing is trusted. Learned, but the
    # prior is deliberately tight — the one-step filter likelihood rewards a wide latent, and at
    # the prior's 97.5th percentile (0.099) the wave-generation ratio is already ~0.5.
    "Rt_sigma_stat" => PriorSpec(mean = 0.05, std = 0.02),
    "R0_baseline" => PriorSpec(mean = 2.0, std = 0.75),                                    # transmission level (learned)
    # Initial latent decline rate (per year). Informed prior from reports/susceptible-reconstruction
    # Table 4 (NSSP ED visits): the 90 % interval runs from "no decline" to "all of the observed
    # decline is ascertainment". Unconstrained, not truncated at 0, so a genuine increase can appear.
    # Observing NHSN admissions instead: mean 0.2775, std 0.1687 via [priors.ascertainment_decline_rate].
    "ascertainment_decline_rate" => PriorSpec(
        mean = ASCERTAINMENT_DECLINE_RATE_PRIOR_MEAN, std = ASCERTAINMENT_DECLINE_RATE_PRIOR_SD,
        constraint = "unconstrained",
    ),
    # Trend bend scale: the one-year 1-sd departure of log-ascertainment from a straight line.
    "ascertainment_trend_wander" => PriorSpec(
        mean = DEFAULT_ASCERTAINMENT_TREND_WANDER, std = 0.05,
    ),
)

# Fixed (non-transmission) transition rates, assigned by category from the net's names.
# Erlang stages run at n/mean_duration, so total mean duration is stage-count invariant.
function build_rate_defaults(pn, n_E, n_I, dur)
    r = Dict{Symbol, Float64}()
    for t in (AlgebraicEpiMech.flatten_symbols(x) for x in tnames(pn))
        s = string(t)
        if startswith(s, "transmission")
            continue
        elseif startswith(s, "O_")
            r[t] = 1.0 / dur.obs_progression
        elseif startswith(s, "E")
            r[t] = n_E / dur.latent
        elseif startswith(s, "I")
            r[t] = n_I / dur.infectious
        elseif s == "R_to_S"
            r[t] = 1.0 / dur.immunity
        else
            error("build_rate_defaults: unmapped transition `$t` — extend the mapping")
        end
    end
    return (; r...)
end

# Force of infection (n_I transmission channels sharing one β; n_I=1 drops the suffix). The
# seasonal multiplier is the calendar-anchored indoor-activity forcing; `t` is
# days-since-window-start.
function make_rates(n_I, forcing::IndoorActivityForcing)
    β(latent, hyper, t) = hyper.gamma * hyper.R0_baseline * latent.Rt *
        forcing(hyper, t) / hyper.N
    n_I == 1 && return (latent, hyper, t) -> (transmission_S_I = β(latent, hyper, t),)
    return (latent, hyper, t) -> (; (Symbol("transmission_S_I$i") => β(latent, hyper, t) for i in 1:n_I)...)
end

# Keep the concrete forcing type in the vector-field closure.
_build_transmission_vf(pn, n_I, forcing::IndoorActivityForcing, defaults) =
    build_petri_vf(pn, make_rates(n_I, forcing); defaults = defaults)

# Initial compartments, coupled to the parameters and to the observation process.
#
# Three separate ideas, all of which have to hold at once for the model to start coherently:
#
# 1. **Susceptibles are anchored on the observed prevalence peak**, where `R_eff = 1`, using
#    `S/N = 1/(R0 * Rt * chi)`. If no peak is identified, fall back to the annual-mean equilibrium
#    `S/N = 1/R0`. The derivation and timing shifts live in `src/model/initialisation.jl`.
#
# 2. **Infected compartments are reconstructed from the data**, undoing ascertainment AND the
#    observation delays, via the shared `initial_infection_state` (src/config.jl — see its
#    docstring for the derivation). `alpha_at(t)` is observations per infection at model time `t`:
#    before the filter starts, the trend seed continues its initial level and rate backwards. The
#    seed uses `alpha_at(0)`, and the anchor uses the value in force when each historical infection
#    happened. `y0` is a weekly count of a partially-ascertained, delayed
#    signal; the compartments hold prevalence of infections. Because the accumulator integrates
#    the infection EVENT, the inversion is just `daily_incidence = y0 / (ascertainment · interval)`
#    — no duration appears in it at all. Each upstream stage then holds
#    `incidence × its own mean duration`, which is why `E ≠ I` unless the latent and infectious
#    periods happen to be equal, and the non-terminal observation stages are primed the same way so
#    the delay pipeline starts full instead of spending its first weeks filling up.
#
# 3. **Everyone else is immune** (`R`), which also gives the waning term `ωR` something to return.
#
# Note the infection transition now reads `S + I -> E + I + O_transmission_1`: the same event,
# additionally recorded. `S + E + I + R = N` still holds — the `O` stages sit outside that budget
# because the accumulator gains a tally entry, not a person.
function build_init(
        layout, y0, pop, n_E, n_I, dur, alpha_at, R0_seed, window;
        history, n_obs_stages::Integer = 1, chi_at = _ -> 1.0, start_at_peak::Bool = false,
    )
    names = ode_names(layout)
    accumulator = only(layout.accumulator_indices)

    recon = initial_infection_state(y0, dur, alpha_at(0.0), window)
    infectious, exposed, obs_stage = recon.infectious, recon.exposed, recon.obs_stage

    has_R = :R in names
    infected = exposed + infectious
    # `nothing` means the supplied history contains no interior peak; use the endemic fallback.
    anchor = peak_anchored_susceptible_fraction(
        history, dur, n_obs_stages, alpha_at, pop, window, chi_at, R0_seed;
        n_E, n_I,
    )
    # `start_at_peak` asserts identity (I) AT the origin — the `t_anchor = 0` case of the same
    # algebra, so the carry is a no-op and `chi` is kept (the endemic fallback drops it).
    susceptible_fraction = start_at_peak ?
        clamp(1 / max(R0_seed * chi_at(0.0), 1.0e-6), 0.0, 1.0) :
        anchor === nothing ? 1 / max(R0_seed, 1.0) :
        anchor.susceptible_fraction
    susceptible = has_R ?
        clamp(pop * susceptible_fraction, 0.0, pop - infected) :
        pop - infected
    immune = max(pop - susceptible - infected, 0.0)
    state = (;
        (
            n => (
                n === :S ? susceptible :
                    n === :R ? immune :
                    startswith(string(n), "E") ? exposed / n_E :
                    startswith(string(n), "I") ? infectious / n_I :
                    # The terminal accumulator is zeroed at the start of every step, so priming
                    # it would be overwritten; only the in-transit stages carry the delay.
                    (startswith(string(n), "O") && i != accumulator) ? obs_stage : 0.0
            ) for (i, n) in enumerate(names)
        )...,
    )
    return (; state, anchor)
end

"""
    build_model(ctx) -> EpiModel

Build the production daily NSSP trend model from a single-series context.
"""
function build_model(ctx)
    epi = ctx.epi
    epi isa BasicSeirDailyNSSPTrendEpiConfig || throw(
        ArgumentError(
            "daily NSSP trend model expected BasicSeirDailyNSSPTrendEpiConfig, " *
                "got $(typeof(epi))"
        )
    )
    priors = ctx.priors
    pop_loc = ctx.pop

    # The trend starts at the value implied on the first model day by the registered reference
    # level and decline rate. This is the same initialization used by the backtested model; after
    # t = 0, ascertainment is exclusively the latent integrated trend below.
    fixed = epi.fixed
    initial_rate = clamp(
        fixed.ascertainment_decline_rate,
        -fixed.ascertainment_rate_bound,
        fixed.ascertainment_rate_bound,
    )
    days_since_reference = value(
        ctx.start_date - parse_ascertainment_reference_date(fixed.ascertainment_reference_date)
    )
    ascertainment_level0 = ascertainment_at(
        fixed.ascertainment,
        fixed.ascertainment_floor_fraction,
        initial_rate,
        days_since_reference,
    )
    ascertainment_seed = TrendAscertainmentSeed(
        ascertainment_level0, fixed.ascertainment_decline_rate,
    )
    ascertainment_rate_prior = priors[ASCERTAINMENT_TREND_RATE]
    ascertainment_latent_specs = ascertainment_trend_specs(
        ; rate_prior = ascertainment_rate_prior, level0 = ascertainment_level0,
    )
    ascertainment_initial_latents = NamedTuple{
        (ASCERTAINMENT_TREND_RATE, ASCERTAINMENT_TREND_LEVEL),
    }((fixed.ascertainment_decline_rate, ascertainment_level0))
    ascertainment_wander = ctx.prior_specs[String(ASCERTAINMENT_TREND_WANDER)].mean
    isfinite(ascertainment_wander) && ascertainment_wander > 0 || throw(
        ArgumentError(
            "prior mean for ascertainment_trend_wander must be positive and finite, " *
                "got $ascertainment_wander"
        )
    )

    model_type = Symbol(epi.model_type)
    ctor = get(COMPARTMENTAL_MODELS, model_type) do
        error("unknown model_type `$(model_type)` (one of $(keys(COMPARTMENTAL_MODELS)))")
    end
    base = ctor(number_E_stages = epi.n_E_stages, number_I_stages = epi.n_I_stages)
    # Observation is attached AFTER the model is built, by pushout: the infection event gains an
    # output into the chain, so it records at its own rate. See
    # `docs/concepts/composition-and-observation.md`.
    pn = attach_observation(
        dom(create_model(OnePopulationTyping(), base)),
        AtEvent(:transmission); n_stages = epi.n_obs_stages,
    )
    rate_defaults = build_rate_defaults(pn, epi.n_E_stages, epi.n_I_stages, epi.durations_days)
    forcing = build_seasonal_forcing(
        PRODUCTION_SEASONALITY, ctx.loc, ctx.start_date; climatology = ctx.climatology,
    )
    # The runner always supplies its plug-in estimate. Wrap the latent ascertainment level with
    # those fixed weekday weights and widen each weekday's observation dispersion accordingly.
    weekday_effects = ctx.day_of_week_effects
    weekday_effects.fallback && error("day-of-week estimation fell back to no effect")
    weekday0 = dayofweek(ctx.start_date)
    weekday_modifier = DayOfWeekModifier(
        TrendAscertainment(), weekday0, weekday_effects.weights,
    )
    weekday_dispersion = DayOfWeekDispersion(weekday0, weekday_effects.extra_var)
    petri_vf! = _build_transmission_vf(pn, epi.n_I_stages, forcing, rate_defaults)
    # AR1 Rt: fixed mean and correlation time, with the initial state and stationary scale drawn
    # from their live priors.
    latent_specs = (
        AR1ParamSpec(
            :Rt; init = priors.Rt, mu = FixedParam(:Rt_mu, epi.fixed.Rt_mu),
            tau = FixedParam(:Rt_tau, epi.fixed.Rt_tau), sigma = priors.Rt_sigma_stat,
        ),
        ascertainment_latent_specs...,
    )
    layout = StateLayout(pn, latent_specs; signal_names = (:reports,))
    stochastic = build_stochastic_update(layout, latent_specs)
    obs_model = (
        SignalObservationSpec(
            1, NegBinomialNoise(phi = weekday_dispersion);
            mean_modifier = weekday_modifier, name = :reports,
        ),
    )
    # Learned parameters must be present here because the PF seeds its θ-cloud from `base_hp`.
    base_hp = (
        gamma = 1.0 / epi.durations_days.infectious, R0_baseline = epi.fixed.R0_baseline,
        N = pop_loc,
        Rt_sigma_stat = epi.fixed.Rt_sigma_stat,
        phi = epi.fixed.phi,
        seasonal_kappa = SEASONAL_KAPPA,
    )
    base_hp = merge(
        base_hp,
        NamedTuple{(ASCERTAINMENT_TREND_WANDER,)}((ascertainment_wander,)),
    )
    # `ctx.i0` is an OBSERVED weekly count; the compartments are infections. Ascertainment is
    # the only thing that sets the implied attack rate, and hence how hard susceptible depletion
    # brakes transmission — a nonlinear effect `Rt` cannot absorb, unlike a level error.
    # The initial state is a FUNCTION OF THE PARAMETERS, not a constant: the equilibrium seed
    # `S(0) = N / R0` only makes `R_eff(0) = 1` if it uses the SAME `R0_baseline` the particle (or
    # the optimizer iterate) is actually carrying. Sharing one `S(0)` across a cloud that varies
    # `R0` means each particle starts at a different `R_eff(0)` — measured spread 0.55 to 1.82
    # across the initial cloud — so particles explore "how supercritical do I start" rather than
    # exploring `R0`, which is a far more consequential axis and badly distorts the likelihood.
    build_initial = function (hyper)
        return build_init(
            # The weekday factor belongs to each count's OBSERVATION date, so it is removed there
            # (`i0` is the observation at t = 0). The reconstruction then inverts with ascertainment
            # alone, at infection times.
            layout, ctx.i0 / day_of_week_weight(weekday_modifier, hyper, 0.0), pop_loc,
            epi.n_E_stages, epi.n_I_stages,
            # Before t = 0, use the straight-line continuation of the trend's initial level/rate.
            epi.durations_days, ascertainment_seed, hyper.R0_baseline, ctx.step_days;
            history = remove_day_of_week_effect(ctx.history, weekday_modifier, hyper),
            n_obs_stages = epi.n_obs_stages, start_at_peak = epi.start_at_peak,
            # Evaluate seasonality at the anchor time; seeded `Rt` is 1.
            chi_at = t -> forcing(hyper, t),
        )
    end
    assemble_x0 = initial -> vcat(
        [initial.state[n] for n in ode_names(layout)],
        collect(stochastic.to_unconstrained(merge((Rt = 1.0,), ascertainment_initial_latents))),
    )
    build_x0 = function (hyper)
        initial = build_initial(hyper)
        return assemble_x0(initial)
    end
    # Learned set: the latent's stationary scale `Rt_sigma_stat`, the mechanistic
    # transmission level, and the ascertainment trend's wander scale. The indoor-activity curve
    # and its strength are fixed model science.
    #
    # `Rt_tau` is fixed: it expresses the timescale on which a year's transmission departs from the
    # climatology (docs/parameter-provenance.md §2.4 and ADR-0003), while the one-step likelihood
    # drives an inferred memory toward zero instead of identifying forecast-horizon persistence.
    #
    # `Rt_sigma_stat` stays learned so each series can say how far it trusts the forcing, but
    # under a tight prior for the same reason (the objective also rewards a wide latent).
    #
    # Watch for: `R0_baseline` was historically pinned at 1 because at population scale S ≈ N
    # barely depletes, so a level above 1 implies sustained exponential growth in the forecast.
    # That is now the data's call rather than an assumption, but it is the first thing to check if
    # forecasts run hot. R0_baseline is also the only level term (μ_R is fixed at 1: a correction
    # to log-seasonality has log-mean zero by construction), so the two do not compete.
    # `phi` is likewise fixed model science and therefore has no prior.
    # The decline rate itself is a filtered latent state; Liu-West learns its wander scale.
    inference_priors = (; (name => priors[name] for name in LEARNED_PARAMETERS)...)

    return EpiModel(;
        vectorfield! = petri_vf!, layout, stochastic, observation = obs_model,
        hyperparams = base_hp, priors = inference_priors, initial_state = build_x0,
        forgetting_memory_days = NamedTuple{(ASCERTAINMENT_TREND_WANDER,)}(
            (DEFAULT_ASCERTAINMENT_TREND_WANDER_MEMORY_DAYS,)
        ),
        # Make the AR1's initial spread follow the `Rt` prior instead of the shared default.
        initial_latent_variance = merge(
            (Rt = prior_unconstrained_variance(priors.Rt),),
            NamedTuple{
                (ASCERTAINMENT_TREND_RATE, ASCERTAINMENT_TREND_LEVEL),
            }(
                (
                    prior_unconstrained_variance(ascertainment_rate_prior),
                    ASCERTAINMENT_TREND_LEVEL_LOG_SD^2,
                )
            ),
        ),
        # The reset accumulator is 0 at t = 0 and only means "this week's incidence" after a week
        # has been integrated — but the filter corrects before it ever predicts, so the first
        # innovation is scored against a predicted observation of ~0. The relative-sd rule would
        # floor its variance at 1e-6, i.e. claim the count is known to ±0.002.
        #
        # An sd of `i0 / ascertainment` says instead: at t = 0 we do not know this week's incidence
        # to better than one whole week of it. That puts the first innovation at about 1 sd, which
        # is what an ensemble filter needs — its compartment/accumulator cross-covariance is sample
        # noise rather than the Gaussian filter's exact zero, so a near-zero innovation variance
        # turns that noise into a gain large enough to diverge. Only the ensemble path reads this;
        # the UKF's `P0` is unchanged.
        initial_accumulator_variance = (
            reports = (ctx.i0 / ascertainment_seed(0.0))^2,
        ),
    )
end
