/*
 * Copyright (c) 2022-2023 Zander Schwid & Co. LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package crecordbase

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"github.com/recordbase/recordbase"
	"github.com/recordbase/recordpb"
	"time"
)

var clientList  recordbase.ClientList

func Connect(commaSeparatedEndpoints, token string, withTls bool, timeoutMillis int) (int, error) {
	clientDeadline := time.Now().Add(time.Duration(timeoutMillis) * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	var tlsConfig *tls.Config
	if withTls {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
			Rand:               rand.Reader,
		}
	}

	client, err := recordbase.NewClient(ctx, commaSeparatedEndpoints, token, tlsConfig)
	if err != nil {
		return -1, err
	}
	return clientList.Add(client), nil
}

func Close(instance int) error {
	return clientList.Remove(instance).Destroy()
}

func Get(instance int, tenant, key string, fileContents bool, timeoutMillis int) (map[string]string, error) {

	clientDeadline := time.Now().Add(time.Duration(timeoutMillis) * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	entry, err := clientList.Get(instance).Get(ctx, &recordpb.GetRequest{
		Tenant:       tenant,
		PrimaryKey:   key,
		FileContents: fileContents,
	})
	if err != nil {
		return nil, err
	}

	resp := make(map[string]string)
	for _, col := range entry.Columns {
		resp[col.Name] = string(col.Value)
	}

	return resp, nil
}





