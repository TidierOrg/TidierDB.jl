module TestTidierDB

using TidierDB
using Test
using Documenter

DocMeta.setdocmeta!(TidierDB, :DocTestSetup, :(using TidierDB); recursive=true)

doctest(TidierDB)

end