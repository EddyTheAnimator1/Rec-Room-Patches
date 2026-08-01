"use strict";

const readline = require("node:readline");
const sharp = require("sharp");
const tf = require("@tensorflow/tfjs");
const nsfw = require("nsfwjs");

let modelPromise;

async function getModel() {
  if (!modelPromise) {
    modelPromise = (async () => {
      await tf.setBackend("cpu");
      await tf.ready();
      return nsfw.load("MobileNetV2");
    })();
  }
  return modelPromise;
}

async function classify(imagePath) {
  const prepared = await sharp(imagePath)
    .rotate()
    .removeAlpha()
    .toColourspace("srgb")
    .resize(224, 224, { fit: "fill" })
    .raw()
    .toBuffer({ resolveWithObject: true });
  const tensor = tf.tensor3d(
    new Uint8Array(prepared.data),
    [prepared.info.height, prepared.info.width, prepared.info.channels],
    "int32"
  );
  try {
    const predictions = await (await getModel()).classify(tensor, 5);
    return Object.fromEntries(
      predictions.map(({ className, probability }) => [className, probability])
    );
  } finally {
    tensor.dispose();
  }
}

const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", async (line) => {
  let request;
  try {
    request = JSON.parse(line);
    const scores = await classify(String(request.path || ""));
    process.stdout.write(JSON.stringify({ id: request.id, ok: true, scores }) + "\n");
  } catch (error) {
    process.stdout.write(
      JSON.stringify({
        id: request && request.id,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      }) + "\n"
    );
  }
});
