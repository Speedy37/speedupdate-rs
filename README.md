# Update system

This update system aim at:

 - efficiency: minimizing download size, cache size, on the fly decompression/patching, resume update process
 - reliability: ensure integrity of updated files, can repair/check integrity of whole working copy
 - static repository: a repository is just a bunch of files (only GET requests)

To achieve thoses goals this library depends on :

 - rust: a memory safe programming language (https://www.rust-lang.org)
 - future-rs: zero-cost Futures in Rust (https://github.com/alexcrichton/futures-rs)
 - hyper: a Modern HTTP library for Rust (https://github.com/hyperium/hyper)
 - native-tls: a wrapper over a platform's native TLS implementation (https://github.com/sfackler/rust-native-tls)
 - vcdiff-rs: for patches (https://github.com/Speedy37/vcdiff-rs)
 - brotli: a very good compressor with fast decompression speed (https://github.com/dropbox/rust-brotli)

## Terminology

 - _operation_: a way to synchronize a file or directory for a given relative file path
 - _version_: a node in the transition graph
 - _package_: an edge between two versions in the transition graph
 - _repository_: a collection of packages and versions forming a transition graph with a current version

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

 - _versions_

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

 - _packages_

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

## Update logic

The update logic is made of 3 stages

 - _prepare_: find the shortest path of packages to download
 - _update_: do the actual update
 - _recover_: if something did go wrong in the _update_ pass, try to fix it
