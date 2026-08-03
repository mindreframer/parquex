defmodule Parquex.MixProject do
  use Mix.Project

  @version "0.3.0"
  @source_url "https://github.com/mindreframer/parquex"

  def project do
    [
      app: :parquex,
      version: @version,
      description: "Parquet files and time datasets on local and S3-compatible storage",
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
      extra_applications: [:crypto, :logger]
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
        {"README.md", [filename: "readme", title: "Parquex"]},
        "CHANGELOG.md",
        "SECURITY.md",
        "docs/stores.md",
        "docs/parquet-files.md",
        "docs/datasets.md",
        "docs/telemetry.md",
        "docs/runtime.md",
        {"docs/architecture/README.md", [filename: "design", title: "Design"]},
        "docs/architecture/storage.md",
        "docs/architecture/streaming.md",
        "docs/architecture/native-runtime.md"
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
