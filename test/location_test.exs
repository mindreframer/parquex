defmodule Parquex.LocationTest do
  use Parquex.FixtureCase, async: true

  alias Parquex.{Error, Location, Object}

  test "normalizes paths and file URIs to local descriptors", %{tmp_dir: tmp_dir} do
    unicode_path = Path.join(tmp_dir, "dáta set.bin")
    encoded_path = String.replace(unicode_path, " ", "%20")

    assert {:ok, path_location} = Location.new(unicode_path)
    assert {:ok, uri_location} = Location.new("file://#{encoded_path}")

    assert path_location.backend == :local
    assert path_location.path == Path.expand(unicode_path)
    assert uri_location.path == path_location.path
    assert URI.to_string(uri_location.uri) |> String.starts_with?("file:///")
  end

  test "reserves normalized S3 descriptors without performing remote work" do
    assert {:ok, location} = Location.new("s3://Example-Bucket/some/key")
    assert location.backend == :s3
    assert URI.to_string(location.uri) == "s3://example-bucket/some/key"

    assert {:error, %Error{category: :unsupported, operation: :head}} = Object.head(location)
  end

  test "preserves caller order and per-location options", %{tmp_dir: tmp_dir} do
    assert {:ok, first} = Location.new(Path.join(tmp_dir, "first"), marker: 1)
    assert {:ok, second} = Location.new(Path.join(tmp_dir, "second"), marker: 2)

    assert {:ok, [normalized_first, normalized_second]} = Location.normalize([first, second])
    assert normalized_first.options.marker == 1
    assert normalized_second.options.marker == 2
  end

  test "redacts credential-shaped and explicitly marked options", %{tmp_dir: tmp_dir} do
    assert {:ok, location} =
             Location.new(Path.join(tmp_dir, "object"),
               secret_access_key: "very-secret-value",
               custom_credential: "also-secret",
               secret_keys: [:custom_credential],
               harmless: "visible"
             )

    inspected = inspect(location)
    assert inspected =~ "[REDACTED]"
    assert inspected =~ "visible"
    refute inspected =~ "very-secret-value"
    refute inspected =~ "also-secret"
  end

  test "invalid credential-bearing URIs return redacted stable errors" do
    secret = "should-never-escape"
    assert {:error, %Error{} = error} = Location.new("s3://user:#{secret}@bucket/key")

    assert error.category == :invalid_argument
    refute inspect(error) =~ secret
  end

  test "validates local bounds and option shapes", %{tmp_dir: tmp_dir} do
    assert {:error, %Error{category: :invalid_argument}} =
             Location.new(Path.join(tmp_dir, "object"), max_range_bytes: 0)

    assert {:error, %Error{category: :invalid_argument}} =
             Location.new(Path.join(tmp_dir, "object"), [:not_a_keyword])

    assert {:error, %Error{category: :invalid_argument}} = Location.new("file:relative.bin")
  end
end
