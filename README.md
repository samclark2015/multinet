# multinet

## Initial Setup
This project is a template for a future app called `multinet`.
Some modifications need to be made for it to do something useful!

-   **All sources & resources go into `multinet/`**  
    Anything out side of this subdirectory does not get installed with the package.
-   **Modify this readme to suit your package!**

## Relative Imports
This is a Python package, and needs to be treated like one during development. This means using Relative Imports &mdash; [Relative Import Info](https://realpython.com/absolute-vs-relative-python-imports/#relative-imports)

## Dependencies
Make sure to list any and all packages *your* package depends on in the `dependencies` list within `setup.py`. If you do not specify all dependent packages here, your package may work within certain Python environments, but not all. 

## A Note on Virtual Environments
To ensure a reproducible build of this package, a virtual environment has been set up in the `venv/` directory. Packages used in the development of your package should be installed here. 
- `source venv/bin/activate[.csh|.fish]`
- `venv/bin/pip install <package>`

## Creating a Release
1. Ensure all changes are commited using `git add` and `git commit`
2. Run `git release` from the terminal and follow prompts
3. Wait about a minute for your package to be assembled on the server (You can check the progress on the Gitlab continuous integration page of your project.)