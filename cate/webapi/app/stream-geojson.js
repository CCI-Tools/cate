/*
 * This modules allows for streaming GeoJSON through a new process.
 * It is designed as a WebWorker (see https://developer.mozilla.org/en-US/docs/Web/API/Worker)
 * and uses OboeJS for JSON streaming.
 *
 * @author Norman Fomferra
 */

importScripts("oboe-browser.js");

const oboe = self.oboe;

self.onmessage = function (event) {
    // console.log('Message received from main script:', event);
    streamFeatures(event.data);
};

function sendData(data) {
    self.postMessage(data);
}

function streamFeatures(url) {
    const featurePackCount = 25;
    let features = [];

    oboe(url)
        .node('features.*', function (feature) {
            if (features.length === featurePackCount) {
                sendData(features);
                features = [];
            }
            features.push(feature);
            // returning oboe.drop means to oboe, it should forget the feature and thus save memory.
            return oboe.drop;
        })
        .done(function (featureCollection) {
            if (features.length) {
                sendData(features);
            }
            // Send sentinel
            sendData(null);
        });
}