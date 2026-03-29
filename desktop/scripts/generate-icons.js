#!/usr/bin/env node
/**
 * Generates app icons (icon.png) from the SVG favicon.
 * Requires: npm install sharp --save-dev (one-time)
 * Usage: node scripts/generate-icons.js
 */
const path = require("path");

async function main() {
  let sharp;
  try {
    sharp = require("sharp");
  } catch {
    console.error("sharp is required: npm install sharp --save-dev");
    process.exit(1);
  }

  const svgPath = path.join(__dirname, "../../ui/public/favicon.svg");
  const buildDir = path.join(__dirname, "../build");

  // Generate 512x512 PNG (electron-builder auto-generates .ico and .icns from this)
  await sharp(svgPath)
    .resize(512, 512)
    .png()
    .toFile(path.join(buildDir, "icon.png"));

  console.log("Generated build/icon.png (512x512)");

  // Generate .ico for Windows (256x256 is the standard)
  await sharp(svgPath)
    .resize(256, 256)
    .png()
    .toFile(path.join(buildDir, "icon.ico.png"));

  console.log("Generated build/icon.ico.png (256x256) — rename to icon.ico or let electron-builder auto-convert");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
