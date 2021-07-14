
## Repository

### Files

 - _current_: a json file containing the current version and description

```json
{
    "version": "1", // repository version
    "current": {
        "revision": "vX.Y.Z", // identifier of the current version
        "description": "",    // description of the current version
    }
}
```

 - _versions_: a json file containing the list of available versions (i.e. changelog)

 ```json
{
    "version": "1", // repository version
    "versions": [   // complete list of versions in the repository
        {
            "revision": "vX.Y.Z", // version identifier
            "description": "",    // version description
        },
        ...
    ]
}
```

 - _packages_: a json file containing the list of available packages (i.e. how to go from one version to another)

```json
{
    "version": "1", // repository version
    "packages": [   // complete list of packages in the repository
        {
            "to": "vX.Y.Z", // version identifier once the package is applied
            "from": "",     // Previous required version identifier, "" if this package is standalone
            "size": "17034889", // Size of the package
        },
        ...
    ]
}
```

 - __${package_name}___.metadata_

A package metadata is both a set of operation to apply a package and a description of the resulting state (ie. the workspace state can be checked with any package metadata leading to the expected version)

```json
{
    "version": "1", // repository version
    "package": {
        "to": "vX.Y.Z", // version identifier once the package is applied
        "from": "",     // Previous required version identifier, "" if this package is standalone
        "size": "17034889", // Size of the package
    },
    "operations": [
        {
            "type": "add", // create a file without prerequirements
            "path": "add_me",

            "dataCompression": "brotli", // compresssion algorithm
            "dataOffset": "13601303", // position of file in the package
            "dataSize": "11536", // size of file in the package
            "dataSha1": "67b8bd13856abd1769f11d2556435b91577d2387", // sha1 hash of file in package

            "finalSha1": "9a7c1f16652ca8ff3a679950ed2d4473a436945b", // sha1 hash of file on disk
            "finalSize": "25088", // size of file on disk
        },
        {
            "type": "patch", // create a file without prerequirements
            "path": "patch_me",

            "dataCompression": "brotli", // compresssion algorithm
            "dataOffset": "13601303", // position of file in the package
            "dataSize": "11536", // size of file in the package
            "dataSha1": "67b8bd13856abd1769f11d2556435b91577d2387", // sha1 hash of file in package

            "patchType": "vcdiff", // patch format
            "localSize": "32256", // size of file on disk before applying the patch
            "localSha1": "2378e74a1674db99b6e4e83e46a120ca0f0916d0", // sha1 hash of file on disk before applying the patch

            "finalSha1": "9a7c1f16652ca8ff3a679950ed2d4473a436945b", // sha1 hash of file on disk
            "finalSize": "25088", // size of file on disk
        },
        {
            "type": "check", // check a file
            "path": "check_me",

            "finalSha1": "9a7c1f16652ca8ff3a679950ed2d4473a436945b", // sha1 hash of file on disk
            "finalSize": "25088", // size of file on disk
        },
        {
            "type": "rm", // remove the file
            "path": "delete_me",
        },
        {
            "type": "mkdir", // create the directory
            "path": "create_me",
        },
        {
            "type": "rmdir", // remove the directory if empty
            "path": "delete_me",
        },
    ]
}
```

 - __${package_name}__

A binary file containing data required by operations as described in the metadata file.
