defmodule Parquex.MixProject do
  use Mix.Project

  def project do
    [
      app: :parquex,
      version: "0.1.0",
      description: "Bounded streaming Parquet for local and S3-compatible immutable objects",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_ignore_filters: [&String.starts_with?(&1, "test/support/")],
      docs: docs(),
      package: package()
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
      {:rustler, "== 0.38.0", runtime: false},
      {:telemetry, "== 1.4.2"},
      {:ex_doc, "== 0.40.3", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "CHANGELOG.md",
        "SECURITY.md",
        "docs/parquet-reads.md",
        "docs/parquet-writes.md",
        "docs/s3.md",
        "docs/append-filtering.md",
        "docs/telemetry.md",
        "docs/release.md"
      ]
    ]
  end

  defp package do
    [
      files: ~w(lib native/parquex_nif/src native/parquex_nif/Cargo.toml
                native/parquex_nif/Cargo.lock README.md CHANGELOG.md SECURITY.md
                LICENSE docs mix.exs rust-toolchain.toml),
      licenses: ["LicenseRef-Proprietary"],
      links: %{
        "Documentation" => "https://hexdocs.pm/parquex",
        "Package" => "https://hex.pm/packages/parquex"
      }
    ]
  end
end
