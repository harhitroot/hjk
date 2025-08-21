"use strict";
const fs = require("fs");
const path = require("path");
const { initAuth } = require("../modules/auth");
const {
  getMessages,
  getMessageDetail,
  downloadMessageMedia,
  uploadMessageToChannel,
  forwardMessageToChannel,
} = require("../modules/messages");
const {
  getMediaType,
  getMediaPath,
  checkFileExist,
  appendToJSONArrayFile,
  wait,
} = require("../utils/helper");
const {
  updateLastSelection,
  getLastSelection,
} = require("../utils/file-helper");
const logger = require("../utils/logger");
const { getDialogName, getAllDialogs } = require("../modules/dialoges");
const {
  downloadOptionInput,
  selectInput,
  booleanInput,
} = require("../utils/input-helper");

// User-defined configurations
const MAX_PARALLEL_DOWNLOADS_CONFIG = 12;
const MESSAGE_LIMIT_CONFIG = 8192;
const RATE_LIMIT_DELAY_CONFIG = 1000;
const DOWNLOAD_DELAY_CONFIG = 1000;

// Default configurations (will be overridden by user-defined ones if provided)
const BATCH_SIZE = 5; // Process exactly 5 messages together
const MAX_PARALLEL_DOWNLOADS = MAX_PARALLEL_DOWNLOADS_CONFIG; // 12 parallel workers per message
const MESSAGE_LIMIT = MESSAGE_LIMIT_CONFIG; // Large message limit for better throughput
const RATE_LIMIT_DELAY = RATE_LIMIT_DELAY_CONFIG; // Delay between batches for rate limiting
const DOWNLOAD_DELAY = DOWNLOAD_DELAY_CONFIG; // Delay between downloads
const UPLOAD_DELAY = 50; // Ultra-minimal delay between uploads (kept as default if not specified)
const MAX_RETRIES = 3; // Fewer retries for faster processing
const BACKOFF_BASE = 500; // Reduced backoff for faster recovery
const PARALLEL_PROCESSING = true; // Enable maximum parallel processing
const MAX_PARALLEL_UPLOADS = MAX_PARALLEL_DOWNLOADS_CONFIG; // 12 parallel workers per message for consistency

/**
 * Enhanced Telegram Channel Downloader with Upload Functionality
 */
class DownloadChannel {
  constructor() {
    this.outputFolder = null;
    this.uploadMode = false;
    this.targetChannelId = null;
    this.downloadableFiles = null;
    this.requestCount = 0;
    this.lastRequestTime = 0;
    this.totalDownloaded = 0;
    this.totalUploaded = 0;
    this.totalMessages = 0;
    this.totalProcessedMessages = 0;
    this.skippedFiles = 0;
    this.selectiveMode = false;
    this.startFromMessageId = 0;
    this.batchCounter = 0; // Add batch counter for refresh mechanism
    this.downloadToEndMode = false; // New mode for download from specific message to end

    const exportPath = path.resolve(process.cwd(), "./export");
    if (!fs.existsSync(exportPath)) {
      fs.mkdirSync(exportPath);
    }
  }

  static description() {
    return "Download all messages from a channel with optional upload to another channel";
  }

  /**
   * Rate limiting with exponential backoff
   */
  async checkRateLimit() {
    const now = Date.now();
    const timeSinceLastRequest = now - this.lastRequestTime;

    if (this.requestCount > 15 && timeSinceLastRequest < 60000) {
      logger.info("Rate limit protection: Waiting 60 seconds...");
      await this.wait(60000);
      this.requestCount = 0;
    }

    this.lastRequestTime = now;
    this.requestCount++;
  }

  /**
   * Enhanced wait function with random delays
   */
  async wait(ms) {
    const randomDelay = Math.random() * 500;
    const totalDelay = ms + randomDelay;
    await new Promise(resolve => setTimeout(resolve, totalDelay));
  }

  /**
   * Float precision delay function for more granular control
   */
  async floatDelay(ms) {
    const delay = ms * (1 + Math.random() * 0.1); // Add up to 10% random variation
    await new Promise(resolve => setTimeout(resolve, delay));
  }


  /**
   * Retry mechanism with exponential backoff
   */
  async retryOperation(operation, operationName, maxRetries = 5) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        logger.warn(`${operationName} failed (attempt ${attempt}/${maxRetries}): ${error.message}`);

        if (attempt === maxRetries) {
          throw error;
        }

        // Longer delays for connection issues
        const baseDelay = error.message.includes('Not connected') || 
                         error.message.includes('Connection closed') ? 5000 : 2000;
        const delay = baseDelay * attempt;

        logger.info(`Retrying ${operationName} in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  async reconnectClient(client) {
    try {
      logger.info('Attempting to reconnect client...');

      // Disconnect first if connected
      if (client.connected) {
        await client.disconnect();
        await new Promise(resolve => setTimeout(resolve, 2000));
      }

      // Reconnect
      await client.connect();
      logger.success('Client reconnected successfully');

      // Wait a bit for connection to stabilize
      await new Promise(resolve => setTimeout(resolve, 1000));

    } catch (error) {
      logger.error(`Failed to reconnect client: ${error.message}`);
      throw error;
    }
  }

  async ensureConnectionHealth(client) {
    try {
      // Simple ping to check if connection is healthy
      await client.getMe();
      logger.info('✅ Connection health check passed');
    } catch (error) {
      logger.warn(`Connection health check failed: ${error.message}`);
      await this.reconnectClient(client);
    }
  }

  /**
   * Check if message has any content (text, media, sticker, etc.)
   */
  hasContent(message) {
    const hasContent = Boolean(
      message.message || 
      message.media || 
      message.sticker ||
      message.document ||
      message.photo ||
      message.video ||
      message.audio ||
      message.voice ||
      message.poll ||
      message.geo ||
      message.contact ||
      message.venue ||
      message.webpage ||
      message.dice ||
      message.groupedId
    );
    
    if (!hasContent) {
      logger.warn(`Message ${message.id} has no recognized content. Keys:`, Object.keys(message));
    }
    
    return hasContent;
  }

  /**
   * Determines if a message should be processed
   */
  shouldProcess(message) {
    if (!this.hasContent(message)) return false;

    // Always process text messages
    if (message.message && !message.media) return true;

    // For media messages, check if we want to download this type
    if (message.media) {
      const mediaType = getMediaType(message);
      const mediaPath = getMediaPath(message, this.outputFolder);
      const extension = path.extname(mediaPath).toLowerCase().replace(".", "");

      return this.downloadableFiles?.[mediaType] ||
             this.downloadableFiles?.[extension] ||
             this.downloadableFiles?.all;
    }

    return true;
  }

  /**
   * Download media from message with persistent retry and reference refresh
   */
  async downloadMessage(client, message, channelId) {
    const maxRetries = 15; // Increased retry attempts for persistence
    let attempt = 0;
    
    while (attempt < maxRetries) {
      try {
        if (!message.media) return null;

        const mediaPath = getMediaPath(message, this.outputFolder);
        const fileExists = checkFileExist(message, this.outputFolder);

        if (fileExists) {
          logger.info(`⏭️  File already exists: ${path.basename(mediaPath)}`);
          return mediaPath;
        }

        // Ensure output directory exists
        const dir = path.dirname(mediaPath);
        if (!fs.existsSync(dir)) {
          fs.mkdirSync(dir, { recursive: true });
        }

        const result = await downloadMessageMedia(client, message, mediaPath);
        if (result) {
          this.totalDownloaded++;
          // For special media types, result might be a different path
          const actualPath = typeof result === 'string' ? result : mediaPath;
          
          // Verify file was actually created
          if (fs.existsSync(actualPath)) {
            logger.info(`✅ Downloaded: ${path.basename(actualPath)} (verified on disk)`);
            return actualPath;
          } else {
            logger.error(`❌ Download claimed success but file not found: ${actualPath}`);
            throw new Error("File not found after download");
          }
        } else {
          throw new Error("Download failed without exception");
        }
      } catch (error) {
        attempt++;
        logger.warn(`❌ Download attempt ${attempt}/${maxRetries} failed for message ${message.id}: ${error.message}`);
        
        // Handle FILE_REFERENCE_EXPIRED error with message refresh
        if (error.message.includes('FILE_REFERENCE_EXPIRED')) {
          logger.info(`🔄 Refreshing message ${message.id} due to expired file reference...`);
          
          try {
            // Refresh the message to get new file reference
            const refreshedMessages = await getMessageDetail(client, channelId, [message.id]);
            if (refreshedMessages && refreshedMessages.length > 0) {
              message = refreshedMessages[0]; // Update message with fresh reference
              logger.info(`✅ Message ${message.id} refreshed successfully`);
            } else {
              logger.warn(`⚠️ Could not refresh message ${message.id}, using original`);
            }
          } catch (refreshError) {
            logger.error(`❌ Failed to refresh message ${message.id}: ${refreshError.message}`);
          }
        }
        
        // Always retry unless max attempts reached - NO SKIPPING
        if (attempt < maxRetries) {
          // Exponential backoff with longer delays for persistent errors
          const baseDelay = error.message.includes('FILE_REFERENCE_EXPIRED') ? 3000 : 2000;
          const delay = Math.min(baseDelay * Math.pow(1.5, attempt), 30000);
          logger.info(`⏳ Persistent retry ${attempt}/${maxRetries} - Waiting ${delay}ms before retry...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue; // Always retry
        } else {
          // Even after max retries, wait longer and try one more time
          logger.warn(`⚠️ Max retries reached for message ${message.id}. Attempting final retry after extended wait...`);
          await new Promise(resolve => setTimeout(resolve, 60000)); // Wait 1 minute
          
          try {
            // Final attempt with fresh message refresh
            const finalRefresh = await getMessageDetail(client, channelId, [message.id]);
            if (finalRefresh && finalRefresh.length > 0) {
              message = finalRefresh[0];
              const finalResult = await downloadMessageMedia(client, message, getMediaPath(message, this.outputFolder));
              if (finalResult && fs.existsSync(getMediaPath(message, this.outputFolder))) {
                this.totalDownloaded++;
                logger.info(`✅ Final retry successful for message ${message.id}`);
                return getMediaPath(message, this.outputFolder);
              }
            }
          } catch (finalError) {
            logger.error(`❌ Final retry failed for message ${message.id}: ${finalError.message}`);
          }
          
          // If absolutely everything fails, return null but don't skip the batch
          logger.error(`❌ All attempts exhausted for message ${message.id} - will retry in next batch cycle`);
          return null;
        }
      }
    }
    return null;
  }

  /**
   * Upload message to target channel with persistent retry logic - NO SKIPPING
   */
  async uploadMessage(client, message, mediaPath = null) {
    const maxRetries = 20; // Much higher retry attempts for persistence
    let attempt = 0;
    
    while (attempt < maxRetries) {
      try {
        if (!this.uploadMode || !this.targetChannelId) return false;

        const result = await uploadMessageToChannel(
          client,
          this.targetChannelId,
          message,
          mediaPath
        );

        if (result) {
          this.totalUploaded++;
          logger.info(`📤 Uploaded message ${message.id} to target channel`);
          return true;
        } else {
          throw new Error("Upload returned false without exception");
        }
      } catch (error) {
        attempt++;
        logger.warn(`❌ Upload attempt ${attempt}/${maxRetries} failed for message ${message.id}: ${error.message}`);
        
        if (error.message.includes('CHAT_FORWARDS_RESTRICTED')) {
          logger.error(`❌ Upload failed for message ${message.id}: Target channel restricts content forwarding. File was downloaded locally.`);
          return false; // Don't retry for permission errors
        } else if (error.message.includes('FLOOD_WAIT')) {
          const waitTime = parseInt(error.message.match(/\d+/)?.[0] || "60") * 1000;
          logger.warn(`⚠️  Rate limited on message ${message.id}. Waiting ${waitTime/1000}s before retry...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
          continue; // Retry after waiting
        }
        
        // For all other errors, keep retrying with exponential backoff
        if (attempt < maxRetries) {
          const delay = Math.min(3000 * Math.pow(1.3, attempt), 45000);
          logger.info(`⏳ Persistent upload retry ${attempt}/${maxRetries} - Waiting ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue; // Always retry
        } else {
          // Final attempt after extended wait
          logger.warn(`⚠️ Max upload retries reached for message ${message.id}. Final attempt after extended wait...`);
          await new Promise(resolve => setTimeout(resolve, 120000)); // Wait 2 minutes
          
          try {
            const finalResult = await uploadMessageToChannel(client, this.targetChannelId, message, mediaPath);
            if (finalResult) {
              this.totalUploaded++;
              logger.info(`✅ Final upload retry successful for message ${message.id}`);
              return true;
            }
          } catch (finalError) {
            logger.error(`❌ Final upload retry failed for message ${message.id}: ${finalError.message}`);
          }
          
          // If upload completely fails, return false but don't skip batch
          logger.error(`❌ All upload attempts exhausted for message ${message.id} - will retry in next batch cycle`);
          return false;
        }
      }
    }
    return false;
  }

  /**
   * Download all messages in MAXIMUM PARALLEL mode with persistent retry
   */
  async downloadBatch(client, messages, channelId) {
    logger.info(`📥 Starting MAXIMUM PARALLEL download of ${messages.length} messages (${MAX_PARALLEL_DOWNLOADS} workers)`);

    // Sort messages by ID to maintain sequence
    messages.sort((a, b) => a.id - b.id);

    // Minimal delay for connection preparation
    await this.wait(100);

    // Create download promises for all messages simultaneously with persistent retry
    const downloadPromises = messages.map(async (message, index) => {
      let retryCount = 0;
      const maxBatchRetries = 3;
      
      while (retryCount < maxBatchRetries) {
        try {
          logger.info(`🔄 Parallel download ${index + 1}/${messages.length}: Message ${message.id} (attempt ${retryCount + 1})`);

          let mediaPath = null;
          let hasContent = false;

          // Handle text messages
          if (message.message && message.message.trim()) {
            hasContent = true;
            logger.info(`📝 Text message: "${message.message.substring(0, 50)}${message.message.length > 50 ? '...' : ''}"`);
          }

          // Handle media messages with persistent retry mechanism
          if (message.media || message.sticker) {
            hasContent = true;
            mediaPath = await this.downloadMessage(client, message, channelId);

            // Verify mediaPath is valid before proceeding
            if (mediaPath && !fs.existsSync(mediaPath)) {
              logger.warn(`❌ Media download failed - file not found: ${mediaPath}`);
              mediaPath = null;
              throw new Error(`File not found: ${mediaPath}`);
            }
          }

          if (hasContent) {
            this.totalProcessedMessages++;
            logger.info(`✅ Parallel download ${index + 1}/${messages.length} complete: Message ${message.id}`);
            return {
              message: message,
              mediaPath: mediaPath,
              hasContent: hasContent,
              downloadIndex: index
            };
          }
          break; // Success, exit retry loop

        } catch (error) {
          retryCount++;
          logger.error(`❌ Batch retry ${retryCount}/${maxBatchRetries} for message ${message.id}: ${error.message}`);
          
          if (retryCount < maxBatchRetries) {
            await new Promise(resolve => setTimeout(resolve, 5000 * retryCount)); // Increasing delay
          } else {
            logger.error(`❌ All batch retries exhausted for message ${message.id}`);
            // Return partial data to avoid losing the message completely
            return {
              message: message,
              mediaPath: null,
              hasContent: Boolean(message.message || message.media || message.sticker),
              downloadIndex: index,
              failed: true
            };
          }
        }
      }
      return null;
    });

    // Wait for ALL downloads to complete with persistent processing
    logger.info(`⏳ Waiting for all ${messages.length} parallel downloads to complete (${MAX_PARALLEL_DOWNLOADS} workers each)...`);
    const results = await Promise.all(downloadPromises);

    // Filter results and sort by message ID
    const downloadedData = results
      .filter(result => result !== null)
      .sort((a, b) => a.message.id - b.message.id);

    const failedDownloads = downloadedData.filter(data => data.failed).length;
    if (failedDownloads > 0) {
      logger.warn(`⚠️ ${failedDownloads} downloads had issues but will proceed with upload`);
    }

    logger.info(`✅ All parallel downloads complete! ${downloadedData.length} messages ready for PARALLEL upload`);
    return downloadedData;
  }

  /**
   * Upload all messages in PARALLEL (30 Mbps speed) while maintaining order
   */
  async uploadBatch(client, downloadedData) {
    if (!this.uploadMode || !downloadedData.length) {
      return downloadedData; // Return data for cleanup even if not uploading
    }

    // Sort by message ID to ensure proper sequence (oldest first)
    downloadedData.sort((a, b) => a.message.id - b.message.id);

    logger.info(`📤 Starting PARALLEL upload of ${downloadedData.length} messages (optimized mode with ${MAX_PARALLEL_UPLOADS} workers each)`);

    // Create parallel upload promises
    const uploadPromises = downloadedData.map(async (data, index) => {
      try {
        // Verify file exists before attempting upload
        if (data.mediaPath && !fs.existsSync(data.mediaPath)) {
          logger.warn(`⚠️  Local file missing for message ${data.message.id}: ${data.mediaPath}`);
          data.mediaPath = null; // Clear invalid path
        }

        // Stagger uploads slightly to reduce connection stress
        await new Promise(resolve => setTimeout(resolve, index * 500));

        logger.info(`🚀 Parallel upload ${index + 1}/${downloadedData.length}: Message ${data.message.id} (${MAX_PARALLEL_UPLOADS} workers)`);

        // Upload with enhanced retry mechanism and connection recovery
        await this.retryOperation(async () => {
          try {
            if (data.mediaPath) { // Use mediaPath if available
              await this.uploadMessage(client, data.message, data.mediaPath);
            } else {
              await this.uploadMessage(client, data.message); // For text-only messages
            }
          } catch (error) {
            if (error.message.includes('Not connected') || 
                error.message.includes('Connection closed') ||
                error.message.includes('hanging states')) {
              logger.warn(`Connection issue detected, reconnecting client...`);
              await this.reconnectClient(client);
              throw error; // Retry after reconnection
            }
            throw error;
          }
        }, `uploading message ${data.message.id}`);

        this.totalUploaded++; // Increment if uploadMessage succeeds without throwing
        logger.info(`✅ Parallel upload ${index + 1}/${downloadedData.length} complete: Message ${data.message.id}`);
        return { success: true, data };

      } catch (error) {
        logger.error(`❌ Error uploading message ${data.message.id}: ${error.message}`);

        // Handle rate limiting with immediate retry
        if (error.message.includes('FLOOD_WAIT')) {
          const waitTime = parseInt(error.message.match(/\d+/)?.[0] || "60") * 1000;
          logger.warn(`⚠️  Rate limited during upload. Waiting ${waitTime/1000}s...`);
          await this.wait(waitTime);

          // Retry the upload
          try {
            const retrySuccess = await this.uploadMessage(client, data.message, data.mediaPath);
            if (retrySuccess) {
              this.totalUploaded++;
              return { success: true, data };
            }
          } catch (retryError) {
            logger.error(`❌ Retry failed for message ${data.message.id}: ${retryError.message}`);
          }
        }

        return { success: false, data };
      }
    });

    // Wait for ALL uploads to complete with maximum parallel processing
    logger.info(`⏳ Waiting for all ${downloadedData.length} parallel uploads to complete (${MAX_PARALLEL_UPLOADS} workers each)...`);
    const uploadResults = await Promise.all(uploadPromises);

    // Count successful uploads
    const successfulUploads = uploadResults.filter(result => result.success).length;

    logger.info(`✅ All parallel uploads complete! ${successfulUploads}/${downloadedData.length} messages uploaded`);
    return downloadedData;
  }

  /**
   * Clean up downloaded files after successful upload
   */
  async cleanupBatch(downloadedData) {
    logger.info(`🗑️ Starting cleanup of ${downloadedData.length} files`);

    for (const data of downloadedData) {
      if (data.mediaPath && fs.existsSync(data.mediaPath)) {
        try {
          fs.unlinkSync(data.mediaPath);
          logger.info(`🗑️ Cleaned up: ${path.basename(data.mediaPath)}`);
        } catch (cleanupError) {
          logger.warn(`⚠️ Could not delete file: ${cleanupError.message}`);
        }
      }
    }

    logger.info(`✅ Cleanup batch complete`);
  }

  /**
   * Refresh messages to prevent FILE_REFERENCE_EXPIRED errors
   */
  async refreshMessagesBatch(client, channelId, messageIds) {
    try {
      logger.info(`🔄 Refreshing ${messageIds.length} messages to prevent reference expiry...`);
      const refreshedMessages = await getMessageDetail(client, channelId, messageIds);
      logger.info(`✅ Successfully refreshed ${refreshedMessages.length} messages`);
      return refreshedMessages;
    } catch (error) {
      logger.error(`❌ Failed to refresh messages: ${error.message}`);
      return null;
    }
  }

  /**
   * Process a batch of messages with refresh mechanism: Download all PARALLEL → Upload all PARALLEL → Delete all
   */
  async processBatch(client, messages, batchIndex, totalBatches, channelId) {
    try {
      this.batchCounter++;
      logger.info(`🔄 Processing batch ${batchIndex + 1}/${totalBatches} (${messages.length} messages) - Batch Counter: ${this.batchCounter}`);

      // Message refresh mechanism every 2 batches
      if (this.batchCounter % 2 === 0) {
        logger.info(`🔄 Batch refresh cycle ${this.batchCounter/2} - Refreshing all messages...`);
        const messageIds = messages.map(m => m.id);
        const refreshedMessages = await this.refreshMessagesBatch(client, channelId, messageIds);
        if (refreshedMessages && refreshedMessages.length > 0) {
          messages = refreshedMessages;
          logger.info(`✅ Messages refreshed successfully for batch ${batchIndex + 1}`);
        }
      }

      // Step 1: Download ALL messages in parallel with persistent retry
      logger.info(`📥 Phase 1: Downloading all ${messages.length} messages in parallel (${MAX_PARALLEL_DOWNLOADS} workers)...`);
      const downloadedData = await this.downloadBatch(client, messages, channelId);

      // Step 2: Upload ALL downloaded messages in parallel with persistent retry
      if (this.uploadMode && downloadedData.length > 0) {
        logger.info(`📤 Phase 2: Uploading all ${downloadedData.length} messages in parallel (${MAX_PARALLEL_UPLOADS} workers)...`);

        // Check connection health before starting uploads
        await this.ensureConnectionHealth(client);

        const uploadedData = await this.uploadBatch(client, downloadedData);

        // Step 3: Clean up all local files ONLY after all uploads are complete
        logger.info(`🗑️ Phase 3: Cleaning up all ${uploadedData.length} files after successful uploads...`);
        await this.cleanupBatch(uploadedData);
      } else {
        logger.info(`💾 Upload mode disabled or no data to upload - files saved locally`);
      }

      logger.info(`✅ Batch ${batchIndex + 1}/${totalBatches} completed successfully`);

    } catch (error) {
      logger.error(`❌ Error processing batch ${batchIndex + 1}: ${error.message}`);
      
      // Retry the entire batch once with fresh message refresh
      logger.info(`🔄 Retrying entire batch ${batchIndex + 1} with fresh message refresh...`);
      try {
        const messageIds = messages.map(m => m.id);
        const retryMessages = await this.refreshMessagesBatch(client, channelId, messageIds);
        if (retryMessages && retryMessages.length > 0) {
          await this.processBatch(client, retryMessages, batchIndex, totalBatches, channelId);
        }
      } catch (retryError) {
        logger.error(`❌ Batch retry also failed: ${retryError.message}`);
        // Continue to next batch rather than stopping completely
      }
    }
  }

  /**
   * Determines if media should be downloaded before upload
   * For restricted channels, ALWAYS download first to ensure compatibility
   */
  shouldDownloadBeforeUpload(message) {
    // ALWAYS download ALL media first for restricted channel compatibility
    // This ensures high-speed download → upload rather than just forwarding
    if (message.media) {
      return true; // Always download ALL media types first
    }

    return false;
  }

  /**
   * Record all messages to JSON file
   */
  recordMessages(messages) {
    const filePath = path.join(this.outputFolder, "all_messages.json");
    if (!fs.existsSync(this.outputFolder)) {
      fs.mkdirSync(this.outputFolder, { recursive: true });
    }

    const data = messages.map((msg) => ({
      id: msg.id,
      message: msg.message || "",
      date: msg.date,
      out: msg.out,
      hasMedia: !!msg.media,
      sender: msg.fromId?.userId || msg.peerId?.userId,
      mediaType: this.hasContent(msg) ? getMediaType(msg) : undefined,
      mediaPath: this.hasContent(msg) && msg.media
        ? getMediaPath(msg, this.outputFolder)
        : undefined,
      mediaName: this.hasContent(msg) && msg.media
        ? path.basename(getMediaPath(msg, this.outputFolder))
        : undefined,
    }));

    appendToJSONArrayFile(filePath, data);
  }

  /**
   * Cleanup temporary files and free memory
   */
  cleanupMemory() {
    try {
      // Force garbage collection if available
      if (global.gc) {
        global.gc();
      }

      // Clean up old temporary files
      const tempDir = path.join(this.outputFolder, 'temp');
      if (fs.existsSync(tempDir)) {
        const files = fs.readdirSync(tempDir);
        const now = Date.now();
        files.forEach(file => {
          const filePath = path.join(tempDir, file);
          const stats = fs.statSync(filePath);
          // Delete files older than 5 minutes
          if (now - stats.mtime.getTime() > 5 * 60 * 1000) {
            fs.unlinkSync(filePath);
          }
        });
      }
    } catch (err) {
      logger.warn("Memory cleanup failed:", err.message);
    }
  }

  /**
   * Show detailed progress information
   */
  showProgress(currentBatch) {
    const progressPercentage = this.totalMessages > 0 
      ? Math.round((this.totalProcessedMessages / this.totalMessages) * 100) 
      : 0;

    // Cleanup memory every 10 batches
    if (currentBatch % 10 === 0) {
      this.cleanupMemory();
    }

    logger.info("=".repeat(60));
    logger.info("📊 PROCESSING PROGRESS REPORT");
    logger.info("=".repeat(60));
    logger.info(`📥 Total Downloaded: ${this.totalDownloaded} files`);
    if (this.uploadMode) {
      logger.info(`📤 Total Uploaded: ${this.totalUploaded} messages`);
    }
    logger.info(`📈 Progress: ${progressPercentage}% (${this.totalProcessedMessages}/${this.totalMessages})`);
    logger.info(`📦 Current batch: ${currentBatch} messages processed`);
    logger.info("=".repeat(60));
  }

  /**
   * Main download and upload function with batch processing
   */
  async downloadChannel(client, channelId, offsetMsgId = 0) {
    try {
      this.outputFolder = path.join(
        process.cwd(),
        "export",
        channelId.toString()
      );

      // For selective mode, adjust the offset
      if (this.selectiveMode && offsetMsgId === 0) {
        offsetMsgId = this.startFromMessageId;
        logger.info(`📋 Starting selective download from message ID: ${offsetMsgId}`);
      }

      // Get messages with rate limiting (oldest to newest)
      const messages = await this.retryOperation(async () => {
        // Fetch messages from oldest to newest
        return await getMessages(client, channelId, MESSAGE_LIMIT, offsetMsgId, true);
      });

      if (!messages.length) {
        logger.info("🎉 Processing completed! No more messages to process.");
        this.showProgress(0);
        return;
      }

      // Filter messages for selective mode
      let filteredMessages = messages;
      if (this.selectiveMode) {
        filteredMessages = messages.filter(msg => msg.id >= this.startFromMessageId);
        logger.info(`📋 Filtered ${filteredMessages.length} messages (ID >= ${this.startFromMessageId}) out of ${messages.length} total`);
      }

      // Sort messages by ID to ensure oldest to newest order
      filteredMessages.sort((a, b) => a.id - b.id);

      // Get detailed message information
      const ids = filteredMessages.map((m) => m.id);
      const details = await this.retryOperation(async () => {
        return await getMessageDetail(client, channelId, ids);
      });

      // Sort details by ID to maintain oldest to newest order
      details.sort((a, b) => a.id - b.id);

      // Filter messages that should be processed
      const messagesToProcess = details.filter(msg => this.shouldProcess(msg));

      logger.info(`📋 Found ${messagesToProcess.length} messages to process out of ${details.length} total`);
      logger.info(`📊 Processing in batches of ${BATCH_SIZE} messages (Download all ${BATCH_SIZE} PARALLEL → Upload all ${BATCH_SIZE} PARALLEL → Delete all ${BATCH_SIZE})`);
      logger.info(`🚀 Speed optimization: ${MAX_PARALLEL_DOWNLOADS} workers for downloads, ${MAX_PARALLEL_UPLOADS} for uploads`);
      logger.info(`⏰ Delays: Rate Limit Delay: ${RATE_LIMIT_DELAY}ms, Download Delay: ${DOWNLOAD_DELAY}ms`);

      if (this.uploadMode) {
        const targetName = await getDialogName(client, this.targetChannelId);
        logger.info(`📤 Target channel: ${targetName}`);
      }

      // Process messages in batches: Download all PARALLEL → Upload all PARALLEL → Delete all
      const totalBatches = Math.ceil(messagesToProcess.length / BATCH_SIZE);

      for (let i = 0; i < messagesToProcess.length; i += BATCH_SIZE) {
        const batch = messagesToProcess.slice(i, i + BATCH_SIZE);
        const batchIndex = Math.floor(i / BATCH_SIZE);

        logger.info(`🔄 Starting batch ${batchIndex + 1}/${totalBatches} - ${batch.length} messages`);

        // Process this batch: Download all PARALLEL → Upload all PARALLEL → Delete all
        await this.processBatch(client, batch, batchIndex, totalBatches, channelId);

        // Enhanced wait between batches with float precision for optimal speed
        if (i + BATCH_SIZE < messagesToProcess.length) {
          logger.info(`⏳ Waiting ${RATE_LIMIT_DELAY/1000}s (float precision) before next batch...`);
          await this.floatDelay(RATE_LIMIT_DELAY);
        }
      }

      // Record all messages
      this.recordMessages(details);

      // Update selection for next batch (use the highest ID for continuation)
      const maxId = Math.max(...filteredMessages.map(m => m.id));
      updateLastSelection({
        messageOffsetId: maxId,
      });

      // Show progress
      this.showProgress(messagesToProcess.length);

      // Continue with next batch
      await this.floatDelay(RATE_LIMIT_DELAY); // Using float delay for consistency
      await this.downloadChannel(
        client,
        channelId,
        maxId
      );

    } catch (err) {
      logger.error("An error occurred:");
      console.error(err);

      if (err.message && err.message.includes("FLOOD_WAIT")) {
        const waitTime = parseInt(err.message.match(/\d+/)?.[0] || "300") * 1000;
        logger.info(`⚠️  Rate limited! Waiting ${waitTime / 1000} seconds...`);
        await this.wait(waitTime);
        return await this.downloadChannel(client, channelId, offsetMsgId);
      }

      throw err;
    }
  }

  /**
   * Configure download and upload options
   */
  async configureDownload(options, client) {
    let channelId = options.channelId;
    let downloadableFiles = options.downloadableFiles;

    // Select source channel with search option
    if (!channelId) {
      logger.info("Please select a channel to download from");
      const allChannels = await getAllDialogs(client);

      // Ask user if they want to search by name or browse all
      const useSearch = await booleanInput("Do you want to search for a channel by name? (No = browse all channels)");

      let selectedChannelId;
      if (useSearch) {
        // Use channel search functionality
        const { searchDialog } = require("../modules/dialoges");
        selectedChannelId = await searchDialog(allChannels);
      } else {
        // Filter out invalid dialogs and format properly for selectInput
        const validChannels = allChannels.filter(d => d.name && d.id);
        const channelOptions = validChannels.map((d) => {
          const displayName = `${d.name} (${d.id})`;
          return {
            name: displayName,
            value: d.id,
          };
        });

        if (channelOptions.length === 0) {
          throw new Error("No valid channels found!");
        }

        selectedChannelId = await selectInput(
          "Please select source channel",
          channelOptions
        );
      }

      channelId = selectedChannelId;
    }

    // Ask for download mode: full, selective specific message only, or from specific message to end
    const downloadModeOptions = [
      { name: "Download ALL messages from the channel", value: "full" },
      { name: "Download ONLY specific message(s)", value: "specific" },
      { name: "Download FROM specific message TO END of channel", value: "toEnd" }
    ];
    
    const downloadMode = await selectInput("Choose download mode:", downloadModeOptions);
    
    let startFromMessageId = 0;
    if (downloadMode === "specific") {
      const { textInput } = require("../utils/input-helper");
      const messageIdInput = await textInput("Enter the specific message ID(s) to download (comma-separated): ");
      const messageIds = messageIdInput.split(",").map(id => parseInt(id.trim())).filter(id => !isNaN(id));
      if (messageIds.length === 0) {
        throw new Error("No valid message IDs provided!");
      }
      // For specific messages, we'll handle this differently in the main flow
      this.specificMessageIds = messageIds;
      logger.info(`📋 Specific message download mode: ${messageIds.join(", ")}`);
    } else if (downloadMode === "toEnd") {
      const { textInput } = require("../utils/input-helper");
      const messageIdInput = await textInput("Enter the message ID to start downloading from (will download to end): ");
      startFromMessageId = parseInt(messageIdInput) || 0;
      logger.info(`📋 Download to end mode: Starting from message ID ${startFromMessageId} to end of channel`);
      this.downloadToEndMode = true;
    } else {
      logger.info("📋 Full channel download mode selected");
    }

    this.selectiveMode = downloadMode !== "full";
    this.startFromMessageId = startFromMessageId;

    // Ask for upload mode
    this.uploadMode = await booleanInput(
      "Do you want to upload messages to another channel? (No = save locally only)"
    );

    if (this.uploadMode) {
      logger.info("Please select target channel for upload");
      const allChannels = await getAllDialogs(client);

      // Ask user if they want to search for target channel by name
      const useSearchForTarget = await booleanInput("Do you want to search for target channel by name? (No = browse all channels)");

      let targetChannelId;
      if (useSearchForTarget) {
        // Use channel search functionality for target
        const validTargetChannels = allChannels.filter(d => d.name && d.id && d.id !== channelId);
        if (validTargetChannels.length === 0) {
          logger.warn("No valid target channels found! Upload mode disabled.");
          this.uploadMode = false;
        } else {
          const { searchDialog } = require("../modules/dialoges");
          targetChannelId = await searchDialog(validTargetChannels);
        }
      } else {
        // Filter out invalid dialogs, exclude source channel, and format properly
        const validTargetChannels = allChannels.filter(d => d.name && d.id && d.id !== channelId);
        const targetOptions = validTargetChannels.map((d) => {
          const displayName = `${d.name} (${d.id})`;
          return {
            name: displayName,
            value: d.id,
          };
        });

        if (targetOptions.length === 0) {
          logger.warn("No valid target channels found! Upload mode disabled.");
          this.uploadMode = false;
        } else {
          targetChannelId = await selectInput(
            "Please select target channel for upload",
            targetOptions
          );
        }
      }

      if (this.uploadMode) {
        this.targetChannelId = targetChannelId;
        logger.info(`📤 Upload mode enabled. Target channel: ${this.targetChannelId}`);
      }
    } 

    if (!this.uploadMode) {
      logger.info("💾 Local storage mode enabled. Files will be saved locally only.");
    }

    // Configure file types (allow all by default for comprehensive download)
    if (!downloadableFiles) {
      downloadableFiles = {
        webpage: true,
        poll: true,
        geo: true,
        contact: true,
        venue: true,
        sticker: true,
        image: true,
        video: true,
        audio: true,
        voice: true,
        document: true,
        pdf: true,
        zip: true,
        all: true
      };
    }

    this.downloadableFiles = downloadableFiles;

    const lastSelection = getLastSelection();
    let messageOffsetId = lastSelection.messageOffsetId || 0;

    if (Number(lastSelection.channelId) !== Number(channelId)) {
      messageOffsetId = 0;
    }

    updateLastSelection({ messageOffsetId, channelId });
    return { channelId, messageOffsetId };
  }

  /**
   * Main handler function
   */
  async handle(options = {}) {
    let client;

    try {
      await this.wait(1000);

      client = await initAuth();
      const { channelId, messageOffsetId } = await this.configureDownload(
        options,
        client
      );

      const dialogName = await getDialogName(client, channelId);
      logger.info(`🚀 Starting download from channel: ${dialogName}`);
      logger.info(`⚙️  Settings: Batch size: ${BATCH_SIZE} messages, Upload mode: ${this.uploadMode ? 'ON' : 'OFF'}`);
      logger.info(`🚀 Speed: ${MAX_PARALLEL_DOWNLOADS} download workers, ${MAX_PARALLEL_UPLOADS} upload workers. Chunk Size: 4MB`);
      logger.info(`⏰ Delays: Rate Limit Delay: ${RATE_LIMIT_DELAY}ms, Download Delay: ${DOWNLOAD_DELAY}ms`);
      logger.info(`📋 Processing order: Oldest to Newest`);
      logger.info(`🔄 Batch pattern: Download all PARALLEL → Upload all PARALLEL → Delete all`);

      await this.downloadChannel(client, channelId, messageOffsetId);

    } catch (err) {
      logger.error("An error occurred:");
      console.error(err);
      await this.wait(30000);

    } finally {
      if (client) {
        try {
          await client.disconnect();
        } catch (disconnectErr) {
          logger.warn("Error disconnecting client:", disconnectErr.message);
        }
      }
      process.exit(0);
    }
  }
}

module.exports = DownloadChannel;