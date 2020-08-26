import typescript from "rollup-plugin-typescript2";
import commonjs from "rollup-plugin-commonjs";
import external from "rollup-plugin-peer-deps-external";
import resolve from "rollup-plugin-node-resolve";
import static_files from "rollup-plugin-static-files";
import babel from "rollup-plugin-babel";
import globals from "rollup-plugin-node-globals";

import pkg from "./package.json";

export default {
  input: "src/__tests__/index.spec.ts",
  output: [
    {
      file: pkg.main,
      format: "cjs",
      exports: "named",
      sourcemap: true,
    },
    {
      file: pkg.module,
      format: "es",
      exports: "named",
      sourcemap: true,
    },
  ],
  plugins: [
    resolve({ browser: true }),
    typescript({
      rollupCommonJSResolveHack: true,
      clean: true,
    }),
    commonjs({
      include: ["node_modules/**"],
    }),
    globals(),
    babel(),
    static_files({
      include: ["src/__tests__/public/"],
    }),
  ],
};
