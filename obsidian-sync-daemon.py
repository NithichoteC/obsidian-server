#!/usr/bin/env python3
"""
Obsidian CouchDB Bidirectional Sync Daemon
Professional implementation with live filesystem watching
"""
import sys
import time
import base64
import logging
from pathlib import Path
import couchdb
import inotify.adapters

# Configuration
COUCHDB_URL = "http://admin:39115677GREYPILLARCDB@localhost:5984"
VAULTS = {
    "personal": {"db": "personal", "path": "/opt/vaults/personal"},
    "greypillar": {"db": "greypillar", "path": "/opt/vaults/greypillar"}
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


class VaultSync:
    def __init__(self, vault_name):
        if vault_name not in VAULTS:
            raise ValueError(f"Unknown vault: {vault_name}")
        
        self.vault_name = vault_name
        cfg = VAULTS[vault_name]
        self.vault_path = Path(cfg["path"])
        
        # Connect to CouchDB
        couch = couchdb.Server(COUCHDB_URL)
        self.db = couch[cfg["db"]]
        logger.info(f"Connected to CouchDB: {cfg['db']}")
        
        # Track syncing state to prevent loops
        self._syncing = set()

    def _doc_id_from_path(self, file_path):
        """Convert file path to CouchDB document ID"""
        rel_path = file_path.relative_to(self.vault_path)
        return str(rel_path).replace("\\", "/")

    def _sync_file_to_db(self, file_path):
        """Sync single file to CouchDB"""
        if not file_path.exists() or not file_path.is_file():
            return
        
        doc_id = self._doc_id_from_path(file_path)
        
        # Prevent circular sync
        if doc_id in self._syncing:
            return
        
        try:
            self._syncing.add(doc_id)
            
            # Read file
            content = file_path.read_text(encoding='utf-8')
            
            # Get or create doc
            try:
                doc = self.db[doc_id]
            except couchdb.http.ResourceNotFound:
                doc = {"_id": doc_id}
            
            # Update doc
            doc["data"] = base64.b64encode(content.encode()).decode()
            doc["mtime"] = int(time.time() * 1000)
            doc["size"] = len(content)
            doc["type"] = "plain"
            
            self.db.save(doc)
            logger.info(f"Synced: {doc_id}")
            
        except Exception as e:
            logger.error(f"Failed to sync {doc_id}: {e}")
        finally:
            self._syncing.discard(doc_id)

    def initial_sync(self):
        """Sync all existing markdown files to CouchDB"""
        logger.info(f"Starting initial sync for {self.vault_name}...")
        
        count = 0
        for md_file in self.vault_path.rglob("*.md"):
            if ".obsidian" in md_file.parts:
                continue
            self._sync_file_to_db(md_file)
            count += 1
        
        logger.info(f"Initial sync complete: {count} files")

    def watch_filesystem(self):
        """Watch filesystem for changes and sync"""
        logger.info(f"Watching filesystem: {self.vault_path}")
        
        i = inotify.adapters.InotifyTree(str(self.vault_path))
        
        for event in i.event_gen(yield_nones=False):
            (_, type_names, path, filename) = event
            
            # Filter markdown files only
            if not filename or not filename.endswith('.md'):
                continue
            
            # Skip .obsidian directory
            if '.obsidian' in path:
                continue
            
            file_path = Path(path) / filename
            
            # Handle file write/create
            if 'IN_CLOSE_WRITE' in type_names or 'IN_MOVED_TO' in type_names:
                logger.info(f"File changed: {filename}")
                self._sync_file_to_db(file_path)
            
            # Handle file delete
            elif 'IN_DELETE' in type_names or 'IN_MOVED_FROM' in type_names:
                try:
                    doc_id = self._doc_id_from_path(file_path)
                    doc = self.db[doc_id]
                    self.db.delete(doc)
                    logger.info(f"Deleted from DB: {doc_id}")
                except couchdb.http.ResourceNotFound:
                    pass

    def run(self):
        """Run sync daemon"""
        logger.info(f"=== Starting Obsidian Sync Daemon for {self.vault_name} ===")
        
        # Initial sync
        self.initial_sync()
        
        # Watch for changes
        try:
            self.watch_filesystem()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 obsidian-sync-daemon.py <vault_name>")
        print(f"Available vaults: {', '.join(VAULTS.keys())}")
        sys.exit(1)
    
    vault = sys.argv[1]
    
    try:
        sync = VaultSync(vault)
        sync.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
