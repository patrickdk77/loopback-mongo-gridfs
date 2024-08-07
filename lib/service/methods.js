'use strict';

const MongoClient = require('mongodb').MongoClient;
const _ = require('lodash');
const h = require('./helpers');

module.exports = {
    // METHODS WITHOUT REMOTES

    // Connect to mongodb if necessary.
    connect: async function (callback) {
        if (!this.db) {
            let url;
            if (!this.options.url) {
                url = (this.options.user && this.options.password) ?
                    'mongodb://{$user}:{$password}@{$host}:{$port}/{$database}' :
                    'mongodb://{$host}:{$port}/{$database}';
                // replace variables
                url = h.replaceInPattern(url, this.options);
            } else {
                url = this.options.url;
            }

            const excludeOptions = ['url', 'username', 'password', 'host', 'port', 'database', 'name', 'connector', 'debug'];
            const validOptions = _.omit(this.options, excludeOptions);
            // connect
            try {
                const client = await MongoClient.connect(url, validOptions);
                this.db = client.db(this.options.database);
                // Set frequently used collections
                this.files = this.db.collection('fs.files');
                this.chunks = this.db.collection('fs.chunks');
                if (callback) callback(null, this.db);
            } catch (error) {
                if (callback) callback(error);
                else throw error;
            }
        }
        return this.db;
    },
    // Find files that match the condition
    // TODO: Replace "where" with "filters"
    find: async function (where) {
        await this.connect();
        return await this.files.aggregate([
            {
                $match: h.convertWhere(where)
            },
            {
                $sort: {uploadDate: -1}
            }
        ]).toArray();
    },
    // Find a single file that matches the condition
    findOne: async function (where) {
        await this.connect();
        const fileMeta = await this.files.findOne(h.convertWhere(where));
        if (!fileMeta) {
            throw h.statusError('File not found', 404);
        }
        return fileMeta
    },
    // Delete all files that match the condition
    delete: async function (where, countFiles = false) {
        await this.connect();
        let versionIds = [];
        let versionsDeleted = 0;
        let filesDeleted = 0;
        const files = await this.files.find( h.convertWhere(where), {_id: 1} ).toArray();
        if (files && files.length) {
            versionIds = files.map(file => file._id);
            const {count} = await this.deleteById(versionIds);
            versionsDeleted = count;
            filesDeleted = _.uniqBy(files, 'filename').length;
        }
        const result = {}
        if (countFiles) {
            result.filesDeleted = filesDeleted
        }
        result.versionsDeleted = versionsDeleted
        return result;
    },
    // Delete multiple file versions by id
    deleteById: async function (versionIds) {
        await this.connect();
        let deletedCount = 0;
        if (versionIds && versionIds.length) {
            const promises = [
                this.files.deleteMany({'_id': {$in: versionIds}}),
                this.chunks.deleteMany({'files_id': {$in: versionIds}})
            ]
            const res = await Promise.all(promises);
            deletedCount = res[0].deletedCount;
        }
        return {
            count: deletedCount
        };
    },
    // Count all files that match the condition
    countFiles: async function (where) {
        await this.connect();
        const count = await this.files.aggregate([
            {$match: h.convertWhere(where)},
            {$group: {_id: "$filename"}},
            {$count: 'count'}
        ]).next();
        return count || {count: 0};
    },
    // Count all versions that match the condition
    countVersions: async function (where) {
        await this.connect();
        const count = await this.files.aggregate([
            {$match: h.convertWhere(where)},
            {
                $group: {
                    _id: null,
                    count: {$sum: 1}
                }
            },
            {
                $project: {
                    _id: 0
                }
            }
        ]).next();
        return count || {count: 0};
    },
    // File upload using file stream
    fileUpload: async function(container, fileStream, filename, mimetype, customMetadata) {
        await this.connect();
        return h.fileUpload(this, container, fileStream, filename, mimetype, customMetadata);
    },

    // METHODS WITH REMOTES

    // List all storage containers
    getContainers: async function () {
        await this.connect();
        return await this.files.distinct('metadata.container');
    },
    // Rename a storage container
    renameContainer: async function (container, newName) {
        await this.connect();
        const where = {'metadata.container': container};
        const filesCount = await this.countFiles(where);
        await this.files.updateMany(
            where,
            {$set: {'metadata.container': newName}}
        );
        return filesCount;
    },
    // Delete a storage container and all files attached to it
    deleteContainer: async function (container) {
        await this.connect();
        return await this.delete({'metadata.container': container}, true);
    },
    // List all files matched by where in storage container
    // TODO: Replace "where" with "filters"
    getContainerFiles: async function (container, where = {}) {
        await this.connect();
        return await this.files.aggregate([
            {
                $match: {
                    $and: [
                        {'metadata.container': container},
                        h.convertWhere(where)
                    ]
                }
            },
            {
                $sort: {uploadDate: -1}
            },
            {
                $group: {
                    _id: '$filename',
                    record: {
                        $first: '$$ROOT'
                    }
                }
            },
            {
                $replaceRoot: {
                    newRoot: '$record'
                }
            }
        ]).toArray();
    },
    // Replace a file within a storage container deleting all of its versions
    replaceContainerFiles: async function (container, req) {
        await this.connect();
        const res = await h.fileUploadFromRequest(this, container, req);
        const promises = res.filter(r => !!r._id).map((r) => {
            return this.delete({
                'metadata.container': container,
                filename: r.filename,
                _id: { neq: r._id }
            })
        })
        await Promise.all(promises);
        return res;
    },
    // Upload a file within a storage container
    uploadContainerFiles: async function (container, req) {
        await this.connect();
        return await h.fileUploadFromRequest(this, container, req);
    },
    // Count all files within a storage container that match the condition
    countContainerFiles: async function (container, where = {}) {
        await this.connect();
        return await this.countFiles({
            $and: [
                {'metadata.container': container},
                h.convertWhere(where)
            ]
        });
    },
    // Download all files matched by where within storage container in zip format
    // TODO: Replace "where" with "filters"
    downloadContainerFiles: async function (container, where = {}, res) {
        const files = await this.getContainerFiles(container, where);
        if (files.length === 0) {
            throw h.statusError('No files in container.', 404);
        }
        return h.zipDownload(this, files, res, container, '{$filename}');
    },
    // Download a single file matched by where within storage container in zip format
    // TODO: Replace "where" with "filters"
    downloadContainerFileWhere: async function (container, where = {}, alias, inline, res) {
        const files = await this.getContainerFiles(container, where);
        if (files.length === 0) {
            throw h.statusError('No files in container.', 404);
        }
        return h.fileDownload(this, files[0], res, inline, alias || '{$filename}');
    },
    // Return a file's metadata by filename within the given container.
    getContainerFile: async function (container, file) {
        await this.connect();
        const fileMeta = await this.files.findOne({
            'filename': file,
            'metadata.container': container
        }, {sort: {'uploadDate': -1}});
        if (!fileMeta) {
            throw h.statusError('File not found.', 404)
        }
        return fileMeta;
    },
    // Delete a file by filename within the given container.
    deleteContainerFile: async function (container, file) {
        await this.connect();
        return await this.delete({'metadata.container': container, 'filename': file}, true);
    },
    // Download a file by filename within the given container.
    downloadContainerFile: async function (container, file, res, alias, inline) {
        const fileMeta = await this.getContainerFile(container, file);
        return h.fileDownload(this, fileMeta, res, inline, alias);
    },
    // List all file versions matched by where in storage container
    // TODO: Replace "where" with "filters"
    getFileVersions: async function (container, file, where = {}) {
        await this.connect();
        return await this.find({
            $and: [
                {'metadata.container': container},
                {'filename': file},
                h.convertWhere(where)
            ]
        }, {sort: {'uploadDate': -1 }});
    },
    // Count all file versions within a storage container that match the condition
    countFileVersions: async function (container, file, where = {}) {
        await this.connect();
        return await this.countVersions({
            $and: [
                {'metadata.container': container},
                {'filename': file},
                h.convertWhere(where)
            ]
        });
    },
    // Download all versions of a file matched by where in zip format
    // TODO: Replace "where" with "filters"
    downloadFileVersions: async function (container, file, alias, where = {}, res) {
        const files = await this.getFileVersions(container, file, where);
        if (files.length === 0) {
            throw h.statusError('No files in container.', 404);
        }
        const namePattern = '{$_id}_' + (alias || '{$filename}');
        return h.zipDownload(this, files, res, alias || file, namePattern);
    },
    // Return a file version's metadata within the given container.
    getFileVersion: async function (container, file, version) {
        await this.connect();
        return await this.findOne({
            '_id': h.convertObjectId(version),
            'metadata.container': container,
            'filename': file
        });
    },
    // Update a file version's metadata within the given container.
    updateFileVersion: async function (container, file, version, metadata) {
        await this.connect();
        const fv = await this.findOne({
            '_id': h.convertObjectId(version),
            'metadata.container': container,
            'filename': file
        });
        if(!fv) throw h.statusError('File not found.', 404)
        return await this.files.updateOne({
            '_id': h.convertObjectId(version),
            'metadata.container': container,
            'filename': file
        },{$set:{
            'metadata': {...fv.metadata, ...metadata, 'container': container}
        }});
    },
    // Delete a file version within the given container.
    deleteFileVersion: async function (container, file, version) {
        await this.connect();
        return await this.delete({'metadata.container': container, 'filename': file, '_id': version});
    },
    // Download a file version within the given container.
    downloadFileVersion: async function (container, file, version, res, alias, inline) {
        const fileMeta = await this.getFileVersion(container, file, version);
        return h.fileDownload(this, fileMeta, res, inline, alias || '{$_id}_{$filename}');
    }
}


