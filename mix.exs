defmodule Dst.MixProject do
  use Mix.Project

  def project do
    [
      app: :dst,
      version: "0.1.0",
      elixir: "~> 1.15",
      erlc_paths: erlc_paths(Mix.env()),
      erlc_options: erlc_options(Mix.env()),
      elixirc_paths: [],
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      dialyzer: dialyzer(),
      deps: deps(),
      package: package(),
      name: "dst",
      description:
        "Deterministic simulation testing for the BEAM: a serializing scheduler, " <>
          "a virtual clock, and a driver that turns a seed into a replayable run.",
      docs: docs()
    ]
  end

  # The library is pure Erlang. `test/support` holds the systems under test the
  # suite drives, which are Erlang too — including two that carry the parse
  # transform, so they have to compile in the same pass as `src`.
  defp erlc_paths(:test), do: ["src", "test/support"]
  defp erlc_paths(_), do: ["src"]

  # The `DST` define is the contract `include/dst.hrl` hangs on: with it, the
  # header brings the parse transform and the `?DST_LOG` macros; without it, it
  # brings nothing. This project's own test suite is a simulation build, so it
  # sets it — the same line every adopter writes.
  defp erlc_options(:test), do: [:debug_info, {:d, :DST}]
  defp erlc_options(_), do: [:debug_info]

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/jessestimpson/dst"},
      files: ["src", "include", "mix.exs", "rebar.config", "README.md", "LICENSE.md", "docs"]
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "LICENSE.md",
        {"docs/01-what-dst-is.md", [title: "What DST is"]},
        {"docs/02-setting-up.md", [title: "Setting up a project"]},
        {"docs/03-two-phase-commit.md", [title: "Example: two-phase commit"]},
        {"docs/04-writing-a-system-under-test.md", [title: "Writing a system under test"]},
        {"docs/05-gotchas.md", [title: "Gotchas and footguns"]},
        {"docs/06-a-journey-through-dst.md", [title: "A Journey Through DST"]},
        {"docs/design.md", [title: "Design history"]}
      ],
      groups_for_extras: [
        Walkthrough: ~r{docs/0\d},
        Design: ~r{docs/design}
      ]
    ]
  end

  # A stable PLT path outside `_build`, so CI can cache it on its own key: the
  # PLT depends on OTP, Elixir and the deps, and not on our source at all.
  defp dialyzer do
    [
      plt_local_path: "priv/plts",
      plt_core_path: "priv/plts"
    ]
  end

  defp aliases do
    [
      lint: [
        "format --check-formatted",
        "cmd rebar3 fmt --check",
        "deps.unlock --check-unused",
        "dialyzer",
        "docs --warnings-as-errors"
      ]
    ]
  end
end
