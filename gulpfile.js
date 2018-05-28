const vesta = require("@vesta/devmaid");
const gulp = require("gulp");

const pkgr = new vesta.Packager({
    root: __dirname,
    src: "src",
    targets: ["es6"],
    files: [".npmignore", "LICENSE", "README.md"],
    publish: "--access=public",
    transform: {
        package: function (package, target, isProduction) {
            if (isProduction) {
                delete package.private;
            }
        },
    }
});

pkgr.createTasks();