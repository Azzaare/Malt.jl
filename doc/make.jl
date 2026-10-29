using Documenter
using Malt

makedocs(
    sitename = "Malt",
    format = Documenter.HTML(repolink = "https://gitlab.naze.baffier.fr/etendu3d/Malt.jl"),
    modules = [Malt],
    pages = ["Malt" => "index.md", "Worker lifecycle" => "lifecycle.md",
        "Verification and limits" => "verification.md"],
)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
if get(ENV, "MALT_DEPLOY_DOCS", "false") == "true"
    deploydocs(repo = ENV["MALT_DOCS_REPO"])
end
