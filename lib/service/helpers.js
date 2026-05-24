'use strict';

const mongodb = require('mongodb');
const GridFSBucket = require('mongodb').GridFSBucket
const Busboy = require('busboy');
const archiver = require('archiver');
const {Readable} = require('stream');

function isPlainObject(value) {
  if (typeof value !== 'object' || value === null) return false

  if (Object.prototype.toString.call(value) !== '[object Object]') return false

  const proto = Object.getPrototypeOf(value);
  if (proto === null) return true

  const Ctor = Object.prototype.hasOwnProperty.call(proto, 'constructor') && proto.constructor;
  return (
      typeof Ctor === 'function' &&
      Ctor instanceof Ctor && Function.prototype.call(Ctor) === Function.prototype.call(value)
  );
}

function get(obj, path, defaultValue = undefined) {
  const travel = regexp =>
    String.prototype.split
      .call(path, regexp)
      .filter(Boolean)
      .reduce((res, key) => (res !== null && res !== undefined ? res[key] : res), obj);
  const result = travel(/[,[\]]+?/) || travel(/[,[\].]+?/);
  return result === undefined || result === obj ? defaultValue : result;
};

const ALLOWED_OPERATORS = new Set([
    'between', 'inq', 'nin', 'like', 'nlike', 'neq', 'regexp',
    'gt', 'gte', 'lt', 'lte', 'in', 'ne', 'exists', 'near', 'maxDistance', 'minDistance', 'geoWithin', 'nearSphere', 'geometry'
]);

function isSafePattern(pattern) {
    if (typeof pattern !== 'string') return true;
    if (pattern.length > 100) return false;
    // Check for nested quantifiers like (a+)+ or (a*)* or (\w+)* which cause catastrophic backtracking
    if (/\([^)]*[*+?{][^)]*\)[*+?{]/.test(pattern)) return false;
    // Check for adjacent repetition characters like ++ or *+
    if (/[*+?{][*+?}]/.test(pattern)) return false;
    return true;
}

function isSafeOptions(options) {
    if (options === null || options === undefined) return true;
    if (typeof options !== 'string') return false;
    if (options.length > 5) return false;
    return /^[gimsuy]*$/.test(options);
}

module.exports = {
    statusError(message, status) {
        const error = new Error(message);
        error.status = status;
        return error;
    },
    replaceInPattern(pattern, obj) {
        return pattern.replace(/{\$([^}]+)}/g, (str, propPath) => {
            let res = str;
            const pathValue = get(obj, propPath);
            if (pathValue !== undefined) {
                res = pathValue;
            } else {
                console.warn(`Path: ${propPath} is undefined`);
            }
            return res;
        })
    },
    fileDownload(ctx, file, res, inline = false, namePattern = '{$filename}') {
        // set headers
        res.set('Content-Type', file.metadata.mimetype);
        res.set('Content-Length', file.length);
        res.set('Content-Disposition', `${(inline) ? 'inline' : 'attachment'};filename="${this.replaceInPattern(namePattern, file).normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^\x20-\x7E]/g, '')}"; filename*=UTF-8''`+encodeURIComponent(this.replaceInPattern(namePattern, file)).replace(/['()*]/g,(c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,).replace(/%(7C|60|5E)/g, (str, hex) => String.fromCharCode(parseInt(hex, 16)),));
        const gfs = new GridFSBucket(ctx.db);
        return gfs.openDownloadStream(file._id);
    },
    fileUpload(ctx, container, fileStream, filename, mimetype, customMetadata = {}) {
        const gfs = new GridFSBucket(ctx.db);
        const fileNameArr = filename.split('.');
        const metadata = Object.assign(customMetadata, {
            container,
            mimetype,
            extension: (fileNameArr.length > 1) ? fileNameArr.at(-1) : ''
        });
        const uploadStream = gfs.openUploadStream(filename, { metadata });
        return new Promise((resolve, reject) => {
            uploadStream.once('finish', (file) => {
                resolve(file);
            });
            uploadStream.on('error', (error) => {
                return reject(error);
            });
            fileStream.pipe(uploadStream);
        })
    },
    async fileUploadFromRequest(ctx, container, req) {
        let {files} = req;
        if (!files) {
            files = await this.parseFilesFromRequest(req);
        }
        const promises = files.map(({buffer, originalname, mimetype, fieldname}) => {
            const fileStream = new Readable({
                read() {
                    this.push(buffer);
                    this.push(null);
                }
            });
            let customMetadata = {};
            if (fieldname && req.body[`${fieldname}_meta`]) {
                customMetadata = JSON.parse(req.body[`${fieldname}_meta`]);
            }
            return this.fileUpload(ctx, container, fileStream, originalname, mimetype, customMetadata);
        });
        return Promise.all(promises);
    },
    zipDownload(ctx, files, res, zipName = 'files', namePattern = '{$filename}') {
        const gfs = new GridFSBucket(ctx.db);
        const archive = archiver('zip', {
            zlib: {level: 9} // Sets the compression level.
        });
        archive.on('warning', function (err) {
            if (err.code === 'ENOENT') {
                console.warn(err);
            } else {
                throw err;
            }
        });
        archive.on('error', function (err) {
            throw err;
        });
        while (files.length) {
            const file = files.pop();
            const fileStream = gfs.openDownloadStream(file._id);
            archive.append(fileStream, {name: this.replaceInPattern(namePattern, file)});
        }
        archive.finalize();

        res.set('Content-Type', 'application/zip');
        res.set('Content-Disposition', `attachment;filename="${zipName.normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^\x20-\x7E]/g, '')}.zip"; filename*=UTF-8''`+encodeURIComponent(`${zipName}.zip`).replace(/['()*]/g,(c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,).replace(/%(7C|60|5E)/g, (str, hex) => String.fromCharCode(parseInt(hex, 16)),));
        return archive;
    },
    convertWhere(where) {
        const query = {};
        if (!isPlainObject(where)) {
            return query;
        }
        Object.keys(where).forEach((k) => {
            // Prevent Prototype Pollution
            if (k === '__proto__' || k === 'constructor' || k === 'prototype') {
                return;
            }
            let cond = where[k];
            if (['and', 'or', 'nor'].includes(k)) {
                if (Array.isArray(cond)) {
                    cond = cond.map((w) => {
                        return this.convertWhere(w);
                    });
                }
                query['$' + k] = cond;
                delete query[k];
                return;
            }
            if (k === 'id') {
                k = '_id';
            }
            let spec = '';
            let regexOptions = null;
            if (isPlainObject(cond)) {
                // Filter out any prototype properties in the sub-object
                const safeKeys = Object.keys(cond).filter(key => key !== '__proto__' && key !== 'constructor' && key !== 'prototype');
                if (safeKeys.length > 0) {
                    spec = safeKeys[0];
                    regexOptions = cond.options;
                    cond = cond[spec];
                } else {
                    return;
                }
            }
            if (spec) {
                // Enforce strict whitelist of allowed operators
                if (!ALLOWED_OPERATORS.has(spec)) {
                    return;
                }
                if (spec === 'between') {
                    query[k] = {$gte: this.typecastValue(k, cond[0]), $lte: this.typecastValue(k, cond[1])};
                } else if (spec === 'inq') {
                    cond = [].concat(cond || []);
                    query[k] = {
                        $in: cond.map((x) => {
                            return this.typecastValue(k, x);
                        }),
                    };
                } else if (spec === 'nin') {
                    cond = [].concat(cond || []);
                    query[k] = {
                        $nin: cond.map((x) => {
                            return this.typecastValue(k, x);
                        }),
                    };
                } else if (spec === 'like') {
                    if (cond instanceof RegExp) {
                        query[k] = {$regex: cond};
                    } else if (typeof cond === 'string' && isSafePattern(cond) && isSafeOptions(regexOptions)) {
                        query[k] = {$regex: cond, $options: regexOptions || ''};
                    }
                } else if (spec === 'nlike') {
                    if (cond instanceof RegExp) {
                        query[k] = {$not: cond};
                    } else if (typeof cond === 'string' && isSafePattern(cond) && isSafeOptions(regexOptions)) {
                        query[k] = {$not: {$regex: cond, $options: regexOptions || ''}};
                    }
                } else if (spec === 'neq') {
                    query[k] = {$ne: this.typecastValue(k, cond)};
                } else if (spec === 'regexp') {
                    if (cond instanceof RegExp) {
                        query[k] = {$regex: cond};
                    } else if (typeof cond === 'string' && isSafePattern(cond)) {
                        if (cond.global) {
                            console.warn('{{MongoDB}} regex syntax does not respect the {{`g`}} flag');
                        }
                        query[k] = {$regex: cond};
                    }
                } else {
                    query[k] = {};
                    query[k]['$' + spec] = this.typecastValue(k, cond);
                }
            } else {
                if (cond === null) {
                    // http://docs.mongodb.org/manual/reference/operator/query/type/
                    // Null: 10
                    query[k] = {$type: 10};
                } else {
                    query[k] = this.typecastValue(k, cond);
                }
            }
        });
        return query;
    },
    convertObjectId(id) {
        return mongodb.ObjectId ? new mongodb.ObjectId(id) : new mongodb.ObjectID(id)
    },
    typecastValue(key, val) {
        if (!this.props) {
            const raw = require('./models').file;
            this.props = {
                raw,
                objectIdFields: Object.keys(raw).filter(p => raw[p].type === 'ObjectID'),
                dateFields: Object.keys(raw).filter(p => raw[p].type === 'date')
            }
        }
        if (this.props.objectIdFields.includes(key))
            return this.convertObjectId(val);
        else if (this.props.dateFields.includes(key)) {
            return new Date(val);
        }
        return val;
    },
    parseFilesFromRequest (req) {
        return new Promise((resolve) => {
            const files = [];
            const busboy = new Busboy({ headers: req.headers });
            busboy.on('file', function(fieldname, file, info) {
              const {filename, encoding, mimetype} = info;
                const fileBufs = [];
                file.on("data", (data) => {
                    fileBufs.push(data);
                });
                file.on("close", () => {
                    if (filename) {
                        files.push({
                            buffer: Buffer.concat(fileBufs),
                            originalname: filename,
                            mimetype
                        });
                    }
                });
            });
            busboy.on('close', function() {
                resolve(files);
            });
            req.pipe(busboy);
        });
    }
}