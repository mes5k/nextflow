/*
 * Copyright (c) 2013-2017, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2017, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.fs.dx

import com.upplication.s3fs.AmazonS3Client
import com.upplication.s3fs.S3FileSystem
import com.upplication.s3fs.S3FileSystemProvider
import com.upplication.s3fs.S3Path
import nextflow.file.FileHelper
import nextflow.fs.dx.api.DxApi
import nextflow.util.KryoHelper
import nextflow.util.PathSerializer
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class DxSerializersTest extends Specification {

    def setupSpec() {
        KryoHelper.register(DxPath, DxPathSerializer)
        KryoHelper.register(DxFileSystem, DxFileSystemSerializer)
        KryoHelper.register(S3Path, PathSerializer)
    }

    def testSerialization() {

        setup:
        DxFileSystemProvider provider = new DxFileSystemProvider('123', Mock(DxApi))
        DxFileSystemProvider.instance = provider

        def fs = new DxFileSystem(provider,'123','sandbox')
        def pathObj = new DxPath(fs, '/home/hola')

        def store = File.createTempFile('dxser',null)
        store.deleteOnExit()

        when:
        KryoHelper.serialize(pathObj, store)
        def copy = (DxPath)KryoHelper.deserialize(store)

        then:
        copy == pathObj
        pathObj.getFileName().toString() == 'hola'
        pathObj.toString() == 'dxfs://sandbox:/home/hola'


        cleanup:
        DxFileSystemProvider.instance = null


    }

    def testS3PathSerialization() {

        setup:
        System.setProperty('AWS_ACCESS_KEY', 'xxx')
        System.setProperty('AWS_SECRET_KEY', 'xxx')

        def uri = URI.create('s3:///bucket-name/index.html')

        def client = Mock(AmazonS3Client)
        def provider = Mock(S3FileSystemProvider)
        def fs = new S3FileSystem(provider,client,'')
        provider.newFileSystem(_,_) >> { fs }
        provider.getPath(_) >> { fs.getPath(uri.getPath()) }
        provider.getScheme() >> 's3'
        FileHelper.providersMap['s3'] = provider


        def s3path = provider.getPath(uri)

        def store = File.createTempFile('s3obj',null)

        when:
        KryoHelper.serialize(s3path, store)
        def copy = (S3Path)KryoHelper.deserialize(store)

        then:
        copy == s3path
        s3path.getFileName().toString() == 'index.html'
        s3path.toString() == '/bucket-name/index.html'
        s3path.toUri() == uri

        cleanup:
        store?.delete()

    }



}
