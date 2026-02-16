import js from "@eslint/js"
import globals from "globals"

const recommendedRules = {
  ...js.configs.recommended.rules,
  "no-unused-vars": ["error", { argsIgnorePattern: "^_" }]
}

export default [
  {
    ignores: [
      "node_modules/**",
      "site/data/**",
      "tmp/**"
    ]
  },
  {
    files: ["site/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: globals.browser
    },
    rules: recommendedRules
  },
  {
    files: ["scripts/**/*.mjs"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        ...globals.node,
        ...globals.browser
      }
    },
    rules: recommendedRules
  }
]
