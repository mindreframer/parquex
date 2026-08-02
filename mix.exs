defmodule Parquex.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/mindreframer/parquex"

  def project do
    [
      app: :parquex,
      version: @version,
      description: "Bounded streaming Parquet for local and S3-compatible immutable objects",
      source_url: @source_url,
      homepage_url: @source_url,
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
      {:rustler, "== 0.38.0", optional: true, runtime: false},
      {:rustler_precompiled, "== 0.8.4"},
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
      files:
        ~w(lib native/parquex_nif/src native/parquex_nif/Cargo.toml
           native/parquex_nif/Cargo.lock README.md CHANGELOG.md SECURITY.md
           LICENSE docs mix.exs rust-toolchain.toml .cargo/config.toml) ++
          Path.wildcard("checksum-Elixir.Parquex.Native.exs"),
      licenses: ["LicenseRef-Proprietary"],
      links: %{
        "Documentation" => "https://hexdocs.pm/parquex",
        "Source" => @source_url,
        "Package" => "https://hex.pm/packages/parquex"
      }
    ]
  end
end
