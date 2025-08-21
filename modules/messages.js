const fs = require("fs");
const path = require("path");
const logger = require("../utils/logger");
const { circularStringify } = require("../utils/helper");

const getMessages = async (client, channelId, limit = 10, offsetId = 0, reverse = false) => {
  if (!client || !channelId) {
    throw new Error("Client and channelId are required");
  }

  try {
    const result = await client.getMessages(channelId, {
      limit,
      offsetId,
      reverse: reverse // Add reverse parameter for oldest to newest
    });
    return result;
  } catch (error) {
    throw new Error(`Failed to get messages: ${error.message}`);
  }
};

const getMessageDetail = async (client, channelId, messageIds) => {
  if (!client || !channelId || !messageIds) {
    throw new Error("Client, channelId, and messageIds are required");
  }

  try {
    const result = await client.getMessages(channelId, { ids: messageIds });
    return result;
  } catch (error) {
    throw new Error(`Failed to get message details: ${error.message}`);
  }
};

/**
 * Download message media with progress display - MAXIMUM OPTIMIZED FOR 30 Mbps
 * @param {Object} client Telegram client
 * @param {Object} message Telegram message
 * @param {string} mediaPath Local file save path
 * @param {number} fileIndex Current file number (1-based)
 * @param {number} totalFiles Total files in this batch
 * @param {Object} options Additional options including channelId for reference refresh
 */
const downloadMessageMedia = async (client, message, mediaPath, fileIndex = 1, totalFiles = 1, options = {}) => {
  const { workers = 12, chunkSize = 4 * 1024 * 1024, workerIndex = 0 } = options;
  const RATE_LIMIT_DELAY = 1000; // Delay in ms for rate limiting
  const DOWNLOAD_DELAY = 1000; // Delay in ms between downloads

  const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

  try {
    if (!client || !message || !mediaPath) {
      logger.error("Client, message, and mediaPath are required");
      return false;
    }

    // Log message details for debugging
    logger.info(`Processing message ${message.id} with media type: ${message.media ? Object.keys(message.media)[0] : 'none'}`);

    if (message.media) {
      // Handle special media types that don't require downloading
      if (message.media.webpage) {
        const webpage = message.media.webpage;
        
        // Ensure the webpage directory exists
        const webpageDir = path.join(path.dirname(mediaPath));
        if (!fs.existsSync(webpageDir)) {
          fs.mkdirSync(webpageDir, { recursive: true });
        }
        
        let urlPath = null;
        if (webpage.url) {
          urlPath = path.join(webpageDir, `${message.id}_webpage.txt`);
          const webpageData = {
            url: webpage.url,
            title: webpage.title || '',
            description: webpage.description || '',
            siteName: webpage.siteName || '',
            type: webpage.type || ''
          };
          fs.writeFileSync(urlPath, JSON.stringify(webpageData, null, 2));
          logger.info(`📄 Saved webpage data: ${path.basename(urlPath)}`);
        }

        // Download webpage photo if available
        if (webpage.photo) {
          mediaPath = path.join(webpageDir, `${message.id}_webpage_image.jpeg`);
          // Continue with photo download below
        } else {
          return urlPath || true; // Return the saved webpage file path or true
        }
      }

      if (message.media.poll) {
        const pollDir = path.dirname(mediaPath);
        if (!fs.existsSync(pollDir)) {
          fs.mkdirSync(pollDir, { recursive: true });
        }
        const pollPath = path.join(pollDir, `${message.id}_poll.json`);
        fs.writeFileSync(
          pollPath,
          circularStringify(message.media.poll, null, 2)
        );
        logger.info(`📊 Saved poll data: ${path.basename(pollPath)}`);
        return pollPath; // Return the saved poll file path
      }

      if (message.media.geo) {
        const geoDir = path.dirname(mediaPath);
        if (!fs.existsSync(geoDir)) {
          fs.mkdirSync(geoDir, { recursive: true });
        }
        const geoPath = path.join(geoDir, `${message.id}_location.json`);
        fs.writeFileSync(
          geoPath,
          JSON.stringify({
            latitude: message.media.geo.lat,
            longitude: message.media.geo.long,
            accuracy: message.media.geo.accuracyRadius || null
          }, null, 2)
        );
        logger.info(`📍 Saved location data: ${path.basename(geoPath)}`);
        return geoPath; // Return the saved location file path
      }

      if (message.media.contact) {
        const contactDir = path.dirname(mediaPath);
        if (!fs.existsSync(contactDir)) {
          fs.mkdirSync(contactDir, { recursive: true });
        }
        const contactPath = path.join(contactDir, `${message.id}_contact.json`);
        fs.writeFileSync(
          contactPath,
          JSON.stringify(message.media.contact, null, 2)
        );
        logger.info(`👤 Saved contact data: ${path.basename(contactPath)}`);
        return contactPath; // Return the saved contact file path
      }

      if (message.media.venue) {
        const venueDir = path.dirname(mediaPath);
        if (!fs.existsSync(venueDir)) {
          fs.mkdirSync(venueDir, { recursive: true });
        }
        const venuePath = path.join(venueDir, `${message.id}_venue.json`);
        fs.writeFileSync(
          venuePath,
          JSON.stringify(message.media.venue, null, 2)
        );
        logger.info(`🏢 Saved venue data: ${path.basename(venuePath)}`);
        return venuePath; // Return the saved venue file path
      }

      const fileName = path.basename(mediaPath);
      const startTime = Date.now();

      await delay(DOWNLOAD_DELAY); // Apply delay before download

      // Handle any unrecognized media types
      if (!message.media.photo && !message.media.document && !message.media.video && 
          !message.media.audio && !message.media.voice && !message.media.webpage &&
          !message.media.poll && !message.media.geo && !message.media.contact && 
          !message.media.venue) {
        logger.warn(`Unrecognized media type for message ${message.id}:`, Object.keys(message.media));
        // Try to download anyway
      }

      // EXACT COPY from original 30 Mbps script + additional optimizations
      await client.downloadMedia(message, {
        outputFile: mediaPath,
        workers: workers,
        chunkSize: chunkSize,
        requestSize: undefined, // Let Telegram choose optimal request size
        partSizeKb: undefined, // Let Telegram choose optimal part size
        dcId: undefined, // Let Telegram choose optimal data center
        progressCallback: (downloaded, total) => {
          if (total > 0) {
            const percent = ((downloaded / total) * 100).toFixed(2);
            const elapsedSeconds = (Date.now() - startTime) / 1000;
            const speedBps = elapsedSeconds > 0 ? (downloaded * 8) / elapsedSeconds : 0; // speed in bps
            const speedMbps = (speedBps / 1000 / 1000).toFixed(1);
            process.stdout.write(
              `\r[${fileIndex}/${totalFiles}] ${fileName}: ${percent}% (${speedMbps} Mbps)`
            );
          }
          if (downloaded === total) {
            process.stdout.write(
              `\n✅ Completed: ${fileName} (${fileIndex}/${totalFiles})\n`
            );
          }
        },
      });

      return true;
    } else if (message.sticker) {
      // Handle stickers with same optimized settings as media
      const stickerPath = path.join(path.dirname(mediaPath), `${message.id}_sticker.webp`);
      await delay(DOWNLOAD_DELAY); // Apply delay before download
      await client.downloadMedia(message, {
        outputFile: stickerPath,
        workers: workers,
        chunkSize: chunkSize,
        progressCallback: (downloaded, total) => {
          if (total > 0) {
            const percent = ((downloaded / total) * 100).toFixed(2);
            process.stdout.write(`\r[${fileIndex}/${totalFiles}] Sticker: ${percent}%`);
          }
          if (downloaded === total) {
            process.stdout.write(`\n✅ Downloaded: Sticker [${fileIndex}/${totalFiles}]\n`);
          }
        },
      });
      return true;
    } else {
      logger.error("No media found in the message");
      return false;
    }

  } catch (err) {
    logger.error(`Error downloading media for message ${message.id}: ${err.message}`);
    return false;
  }
};

/**
 * Upload a message with media to a target channel with preserved caption/text
 * Optimized for 30 Mbps upload speed with proper video handling
 * @param {Object} client Telegram client
 * @param {string} targetChannelId Target channel ID
 * @param {Object} message Original message object
 * @param {string} mediaPath Local media file path (optional)
 */
const uploadMessageToChannel = async (client, targetChannelId, message, mediaPath = null) => {
  try {
    if (!client || !targetChannelId || !message) {
      throw new Error("Client, targetChannelId, and message are required");
    }

    // Preserve original caption/text exactly as it appears
    const originalCaption = message.message || "";
    const originalEntities = message.entities || [];

    let uploadOptions = {
      message: originalCaption,
      entities: originalEntities,
      parseMode: null, // Use entities instead of parseMode for exact preservation
      silent: true,
      uploadStartTime: Date.now(),
      // OPTIMIZED for 30+ Mbps upload speed - matching download settings
      workers: 12, // Same as download for consistency
      chunkSize: 4 * 1024 * 1024, // Same as download for consistency
      partSizeKb: undefined, // Let Telegram choose optimal part size
      bigFileThreshold: 1 * 1024 * 1024, // Use big file upload for files > 1MB
      progressCallback: (uploaded, total) => {
        if (total > 0) {
          const percent = ((uploaded / total) * 100).toFixed(1);
          const elapsedSeconds = (Date.now() - uploadOptions.uploadStartTime) / 1000;
          const speedBps = (uploaded * 8) / elapsedSeconds; // speed in bps
          const speedMbps = (speedBps / 1000 / 1000).toFixed(1);
          process.stdout.write(`\r📤 Uploading: ${percent}% (${speedMbps} Mbps)`);
        }
        if (uploaded === total) {
          const elapsedSeconds = (Date.now() - uploadOptions.uploadStartTime) / 1000;
          const avgSpeedMbps = ((total * 8) / elapsedSeconds / 1000 / 1000).toFixed(1);
          process.stdout.write(`\n📤 Upload complete - Avg: ${avgSpeedMbps} Mbps\n`);
        }
      }
    };

    // Handle different types of content
    if (message.media) {
      // Handle media messages with preserved captions
      if (mediaPath && fs.existsSync(mediaPath)) {
        // Upload with local file (for downloaded media)
        uploadOptions.file = mediaPath;

        // Preserve media-specific attributes for different media types
        if (message.media.photo) {
          uploadOptions.supportsStreaming = true;
          logger.info(`📸 Uploading photo: ${path.basename(mediaPath)}`);
        } else if (message.media.document) {
          const doc = message.media.document;
          uploadOptions.attributes = doc.attributes || [];
          uploadOptions.mimeType = doc.mimeType;
          uploadOptions.supportsStreaming = true;

          // Handle video documents (MP4, AVI, MKV, etc.)
          const videoExtensions = ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp'];
          const audioExtensions = ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a', '.opus'];
          const fileExt = path.extname(mediaPath).toLowerCase();

          if (videoExtensions.includes(fileExt)) {
            uploadOptions.supportsStreaming = true;
            uploadOptions.videoNote = doc.videoNote || false;
            // Preserve video attributes
            if (doc.attributes) {
              const videoAttr = doc.attributes.find(attr => attr.className === 'DocumentAttributeVideo');
              if (videoAttr) {
                uploadOptions.duration = videoAttr.duration;
                uploadOptions.width = videoAttr.w;
                uploadOptions.height = videoAttr.h;
                uploadOptions.roundMessage = videoAttr.roundMessage;
              }
            }
            logger.info(`🎥 Uploading video: ${path.basename(mediaPath)}`);
          } else if (audioExtensions.includes(fileExt)) {
            // Preserve audio attributes
            if (doc.attributes) {
              const audioAttr = doc.attributes.find(attr => attr.className === 'DocumentAttributeAudio');
              if (audioAttr) {
                uploadOptions.duration = audioAttr.duration;
                uploadOptions.performer = audioAttr.performer;
                uploadOptions.title = audioAttr.title;
                uploadOptions.voice = audioAttr.voice;
              }
            }
            logger.info(`🎵 Uploading audio: ${path.basename(mediaPath)}`);
          } else {
            logger.info(`📄 Uploading document: ${path.basename(mediaPath)}`);
          }
        } else if (message.media.video) {
          // Handle direct video messages
          uploadOptions.supportsStreaming = true;
          uploadOptions.videoNote = message.media.videoNote || false;
          if (message.media.video.duration) {
            uploadOptions.duration = message.media.video.duration;
          }
          if (message.media.video.w && message.media.video.h) {
            uploadOptions.width = message.media.video.w;
            uploadOptions.height = message.media.video.h;
          }
          logger.info(`🎥 Uploading video message: ${path.basename(mediaPath)}`);
        }

      } else {
        // If no local file available, this will likely fail for restricted channels
        logger.warn(`⚠️  No local file available for message ${message.id}, upload may fail on restricted channels`);
        return false;
      }

      // Handle special media types
      if (message.media.poll) {
        // For polls, create a text message with poll data
        const pollData = message.media.poll;
        uploadOptions.message = `📊 Poll: ${pollData.question}\n\nOptions:\n${pollData.answers.map((ans, i) => `${i + 1}. ${ans.text}`).join('\n')}\n\n${originalCaption}`;
        delete uploadOptions.file;
      } else if (message.media.geo) {
        // For location, send as venue or text
        const geo = message.media.geo;
        uploadOptions.message = `📍 Location: ${geo.lat}, ${geo.long}\n\n${originalCaption}`;
        delete uploadOptions.file;
      } else if (message.media.contact) {
        // For contacts, send contact info
        const contact = message.media.contact;
        uploadOptions.message = `👤 Contact: ${contact.firstName} ${contact.lastName || ''}\nPhone: ${contact.phoneNumber}\n\n${originalCaption}`;
        delete uploadOptions.file;
      } else if (message.media.venue) {
        // For venues, send venue info
        const venue = message.media.venue;
        uploadOptions.message = `🏢 Venue: ${venue.title}\nAddress: ${venue.address}\n\n${originalCaption}`;
        delete uploadOptions.file;
      } else if (message.media.webpage) {
        // For web pages, include URL
        const webpage = message.media.webpage;
        uploadOptions.message = `🔗 ${webpage.title || 'Webpage'}\n${webpage.url}\n${webpage.description || ''}\n\n${originalCaption}`;
        delete uploadOptions.file;
      }

    } else if (message.sticker) {
      // Handle stickers
      uploadOptions.file = message.sticker;
      uploadOptions.sticker = true;
    } else {
      // Text-only message
      if (!originalCaption.trim()) {
        logger.warn(`Message ${message.id} has no content to upload`);
        return false;
      }
    }

    // Send the message with preserved formatting
    let result;
    if (uploadOptions.file && fs.existsSync(uploadOptions.file)) {
      // For media files, use sendFile which properly handles video/audio/document uploads
      result = await client.sendFile(targetChannelId, {
        file: uploadOptions.file,
        caption: uploadOptions.message,
        entities: uploadOptions.entities,
        supportsStreaming: uploadOptions.supportsStreaming,
        duration: uploadOptions.duration,
        width: uploadOptions.width,
        height: uploadOptions.height,
        mimeType: uploadOptions.mimeType,
        attributes: uploadOptions.attributes,
        videoNote: uploadOptions.videoNote,
        performer: uploadOptions.performer,
        title: uploadOptions.title,
        voice: uploadOptions.voice,
        workers: uploadOptions.workers,
        chunkSize: uploadOptions.chunkSize,
        progressCallback: uploadOptions.progressCallback,
        silent: true
      });
    } else {
      // For text-only messages or when no file is available
      result = await client.sendMessage(targetChannelId, uploadOptions);
    }
    return result;

  } catch (error) {
    throw new Error(`Failed to upload message: ${error.message}`);
  }
};

/**
 * Forward a message to target channel
 * @param {Object} client Telegram client
 * @param {string} targetChannelId Target channel ID
 * @param {string} sourceChannelId Source channel ID
 * @param {number} messageId Message ID to forward
 */
const forwardMessageToChannel = async (client, targetChannelId, sourceChannelId, messageId) => {
  try {
    const result = await client.forwardMessages(targetChannelId, {
      messages: [messageId],
      fromPeer: sourceChannelId,
      silent: true
    });
    return result;
  } catch (error) {
    throw new Error(`Failed to forward message: ${error.message}`);
  }
};

// Helper function to determine if media should be downloaded before upload
/**
 * Determines if media should be downloaded before upload
 * For videos and large files, we might want to download first for better quality
 */
function shouldDownloadBeforeUpload(message) {
  if (!message.media) return false;

  // Always download videos and documents to ensure full quality upload
  if (message.media.video ||
    (message.media.document && message.media.document.mimeType &&
      (message.media.document.mimeType.startsWith('video/') ||
        message.media.document.mimeType.startsWith('audio/')))) {
    return true;
  }

  // Download large files to avoid telegram compression
  if (message.media.document && message.media.document.size > 10 * 1024 * 1024) { // > 10MB
    return true;
  }

  return false;
}

module.exports = {
  getMessages,
  getMessageDetail,
  downloadMessageMedia,
  uploadMessageToChannel,
  forwardMessageToChannel,
};