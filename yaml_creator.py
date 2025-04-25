import pkg_resources
import yaml

# Get list of installed packages
installed_packages = {
    dist.project_name: dist.version
    for dist in pkg_resources.working_set
}

# Write to YAML
with open("requirements.yaml", "w") as f:
    yaml.dump({"dependencies": installed_packages}, f)
