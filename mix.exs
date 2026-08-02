defmodule Parquex.MixProject do
  use Mix.Project

  def project do
    [
      app: :parquex,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_ignore_filters: [&String.starts_with?(&1, "test/support/")]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:rustler, "== 0.38.0", runtime: false}
    ]
  end
end
