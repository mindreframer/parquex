ExUnit.start(exclude: [rustfs_integration: true])

Code.require_file("support/fixture_case.ex", __DIR__)
Code.require_file("support/resource_leak.ex", __DIR__)
Code.require_file("support/rustfs_case.ex", __DIR__)
