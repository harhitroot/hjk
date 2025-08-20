const mimeDB = require("mime-db");
const fs = require("fs");
const path = require("path");

// Define media types
const MEDIA_TYPES = {
  IMAGE: "image",
  VIDEO: "video",
  AUDIO: "audio",
  WEBPAGE: "webpage",
  POLL: "poll",
  GEO: "geo",
  VENUE: "venue",
  CONTACT: "contact",
  STICKER: "sticker",
  DOCUMENT: "document",
  OTHERS: "others",
};

// Define console colors for logging
const consoleColors = {
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m",
  cyan: "\x1b[36m",
  white: "\x1b[37m",
  reset: "\x1b[0m",
};

// Get the media type of a message
const getMediaType = (message) => {
  if (!message.media) {
    return null;
  }

  if (message.media.webpage) {
    return MEDIA_TYPES.WEBPAGE;
  }

  if (message.media.poll) {
    return MEDIA_TYPES.POLL;
  }

  if (message.media.geo) {
    return MEDIA_TYPES.GEO;
  }

  if (message.media.contact) {
    return MEDIA_TYPES.CONTACT;
  }

  if (message.media.venue) {
    return MEDIA_TYPES.VENUE;
  }

  if (message.sticker) {
    return MEDIA_TYPES.STICKER;
  }

  if (message.media.photo) {
    return MEDIA_TYPES.IMAGE;
  }

  if (message.media.document) {
    const document = message.media.document;
    const mimeType = document.mimeType;

    if (mimeType) {
      if (mimeType.startsWith("image/")) {
        return MEDIA_TYPES.IMAGE;
      } else if (mimeType.startsWith("video/")) {
        return MEDIA_TYPES.VIDEO;
      } else if (mimeType.startsWith("audio/")) {
        return MEDIA_TYPES.AUDIO;
      } else {
        return "document";
      }
    }

    // Comprehensive video and audio extension support
    const fileName = document.fileName || "";
    const extension = path.extname(fileName).toLowerCase().replace(".", "");

    // Comprehensive video extensions
    const videoExtensions = [
      "mp4", "avi", "mkv", "mov", "wmv", "flv", "webm", "m4v", "3gp", "3g2",
      "asf", "divx", "f4v", "m2v", "m4p", "m4v", "mpg", "mpeg", "mpv", "mxf",
      "ogv", "rm", "rmvb", "swf", "ts", "vob", "xvid", "yuv", "qt", "mts",
      "m2ts", "dv", "amv", "bik", "dnxhd", "hevc", "prores", "roq", "smv"
    ];

    // Comprehensive audio extensions  
    const audioExtensions = [
      "mp3", "wav", "flac", "aac", "ogg", "wma", "m4a", "opus", "aiff", "au",
      "ra", "amr", "3ga", "ac3", "ape", "caf", "dts", "m4b", "m4p", "m4r",
      "mka", "mp2", "mpc", "oga", "spx", "tta", "voc", "vox", "w64", "wv",
      "xa", "gsm", "dss", "msv", "dvf", "mmf", "vqf", "awb", "amz", "aa",
      "aa3", "aax", "act", "aiff", "alac", "ape", "awb", "dct", "dss", "dvf",
      "flac", "gsm", "iklax", "ivs", "m4a", "m4b", "m4p", "mmf", "mpc", "msv",
      "nmf", "nsf", "ogg", "oga", "mogg", "opus", "ra", "rm", "raw", "rf64",
      "sln", "tta", "voc", "vox", "wav", "wma", "wv", "webm", "8svx", "cda"
    ];

    // Image extensions
    const imageExtensions = [
      "jpg", "jpeg", "png", "gif", "bmp", "webp", "tiff", "tif", "svg", "ico",
      "psd", "raw", "cr2", "nef", "orf", "sr2", "eps", "ai", "indd", "heic",
      "heif", "avif", "jxl", "jp2", "j2k", "jpf", "jpx", "jpm", "mj2"
    ];

    if (imageExtensions.includes(extension)) {
      return MEDIA_TYPES.IMAGE;
    } else if (videoExtensions.includes(extension)) {
      return MEDIA_TYPES.VIDEO;
    } else if (audioExtensions.includes(extension)) {
      return MEDIA_TYPES.AUDIO;
    } else {
      return extension || "document";
    }
  }

  return null;
};

// Check if a file already exists (improved for collision handling)
const checkFileExist = (message, outputFolder) => {
  if (!message || !message.media) return false;

  let fileName = `${message.id}_file`;
  const { media } = message;

  if (media.document) {
    const docAttributes = media.document.attributes;
    if (docAttributes) {
      const fileNameObj = docAttributes.find(
        (e) => e.className === "DocumentAttributeFilename"
      );
      if (fileNameObj) {
        fileName = fileNameObj.fileName;
      } else {
        const ext = mimeDB[media.document.mimeType]?.extensions[0];
        if (ext) fileName += `.${ext}`;
      }
    }
  }

  if (media.video) fileName += ".mp4";
  if (media.audio) fileName += ".mp3";
  if (media.photo) fileName += ".jpg";

  const folderType = filterString(getMediaType(message));
  
  // Check for unique filename with message ID (consistent with getMediaPath)
  const ext = path.extname(fileName);
  const baseName = path.basename(fileName, ext);
  const uniqueFileName = `${baseName}_${message.id}${ext}`;
  const filePath = path.join(outputFolder, folderType, uniqueFileName);

  return fs.existsSync(filePath);
};

// Get the path to save the media file (improved collision handling)
const getMediaPath = (message, outputFolder) => {
  if (!message || !message.media) return "unknown";

  let fileName = `${message.id}_file`;
  const { media } = message;

  if (media.document) {
    const docAttributes = media.document.attributes;
    if (docAttributes) {
      const fileNameObj = docAttributes.find(
        (e) => e.className === "DocumentAttributeFilename"
      );
      if (fileNameObj) {
        fileName = fileNameObj.fileName;
      } else {
        const ext = mimeDB[media.document.mimeType]?.extensions[0];
        if (ext) fileName += `.${ext}`;
      }
    }
  }

  if (media.video) fileName += ".mp4";
  if (media.audio) fileName += ".mp3";
  if (media.photo) fileName += ".jpg";

  const folderType = filterString(getMediaType(message));
  
  // Always use message ID in filename to avoid collisions entirely
  const ext = path.extname(fileName);
  const baseName = path.basename(fileName, ext);
  const uniqueFileName = `${baseName}_${message.id}${ext}`;
  
  const finalPath = path.join(outputFolder, folderType, uniqueFileName);
  
  // Ensure directory exists
  if (!fs.existsSync(path.dirname(finalPath))) {
    fs.mkdirSync(path.dirname(finalPath), { recursive: true });
  }

  return finalPath;
};

// Get the type of dialog
const getDialogType = (dialog) => {
  if (dialog.isChannel) return "Channel";
  if (dialog.isGroup) return "Group";
  if (dialog.isUser) return "User";
  return "Unknown";
};

// Logging utility
const logMessage = {
  info: (message, icon=true) => {
    console.log(`📢: ${consoleColors.magenta}${message}${consoleColors.reset}`);
  },
  error: (message) => {
    console.log(`❌ ${consoleColors.red}${message}${consoleColors.reset}`);
  },
  success: (message) => {
    console.log(`✅ ${consoleColors.cyan}${message}${consoleColors.reset}`);
  },
  debug: (message) => {
    console.log(`⚠️ ${message}`);
  },
};

// Wait for a specified number of seconds
const wait = (seconds) => {
  return new Promise((resolve) => {
    setTimeout(resolve, seconds * 1000);
  });
};

// Filter a string to remove non-alphanumeric characters
const filterString = (string) => {
  return string.replace(/[^a-zA-Z0-9]/g, "");
};

// Stringify an object with circular references
const circularStringify = (obj, indent = 2) => {
  const cache = new Set();
  const retVal = JSON.stringify(
    obj,
    (key, value) =>
      typeof value === "object" && value !== null
        ? cache.has(value)
          ? undefined
          : cache.add(value) && value
        : value,
    indent
  );
  cache.clear();
  return retVal;
};

// Append data to a JSON array file
const appendToJSONArrayFile = (filePath, dataToAppend) => {
  try {
    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, circularStringify(dataToAppend, null, 2));
    } else {
      const data = fs.readFileSync(filePath);
      const json = JSON.parse(data);
      json.push(dataToAppend);
      fs.writeFileSync(filePath, circularStringify(json, null, 2));
    }
  } catch (e) {
    logMessage.error(`Error appending to JSON Array file ${filePath}`);
    console.error(e);
  }
};

// Cleanup a file after use
const cleanupFile = (filePath) => {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      logMessage.success(`Cleaned up file: ${filePath}`);
    }
  } catch (error) {
    logMessage.error(`Error cleaning up file ${filePath}: ${error.message}`);
  }
};

// Get a temporary media path for downloading files
const getTempMediaPath = (message) => {
  const folderType = filterString(getMediaType(message));
  const fileName = `${message.id}_temp_file`;
  const tempDir = path.join(__dirname, 'temp', folderType);

  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true });
  }

  return path.join(tempDir, fileName);
};

module.exports = {
  getMediaType,
  getDialogType,
  logMessage,
  circularStringify,
  getMediaPath,
  checkFileExist,
  appendToJSONArrayFile,
  wait,
  filterString,
  cleanupFile,
  getTempMediaPath,
};