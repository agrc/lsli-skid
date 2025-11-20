# Changelog

## [1.0.1](https://github.com/agrc/lsli-skid/compare/v1.0.0...v1.0.1) (2025-11-20)


### Dependencies

* q4 dbot, dbot frequency ([39eb9c2](https://github.com/agrc/lsli-skid/commit/39eb9c2531dda28a8d334475286e5f0b8c11d777))

## 1.0.0 (2025-06-06)


### Features

* load links to interactive maps ([d66f8a4](https://github.com/agrc/lsli-skid/commit/d66f8a4e4acbf28009371c72e6dfe8ff85868f03))
* load points from graphql endpoint ([e17ce1e](https://github.com/agrc/lsli-skid/commit/e17ce1e1e23dfa19a8687fbcfcd06142ea8c05f9))
* load service area data from gsheet ([6b3b2d9](https://github.com/agrc/lsli-skid/commit/6b3b2d9c2db5b35892adfec1d5a2fadfeb620f53))
* use geopandas for projection speed ([9b66443](https://github.com/agrc/lsli-skid/commit/9b66443f93112cd6dd44d6369eb68f656f30b8b6))


### Bug Fixes

* better merge systems, links ([95e57a6](https://github.com/agrc/lsli-skid/commit/95e57a6f3549ff13279b49827c23804ddda057c3))
* drop unneeded links columns ([81cf5f5](https://github.com/agrc/lsli-skid/commit/81cf5f5139fb39aa03221b6652426d0c7e7ec9ae))
* empty rows in links sheet ([af03adf](https://github.com/agrc/lsli-skid/commit/af03adff50defcfe2ab24ecbb5cc01537065b223))
* log and drop missing coords ([09e67c0](https://github.com/agrc/lsli-skid/commit/09e67c05abaa1621fbc2dab9cddc7b5a650069bf))
* more robust null DWSYSNUM check ([13d4e75](https://github.com/agrc/lsli-skid/commit/13d4e759f64b8ec4a6a620dfa5417944982405c1))
* not a method ([31c6cc5](https://github.com/agrc/lsli-skid/commit/31c6cc5c0b64b3f1e2fee8e718bf4add489f2dab))
* switch x/y fields for utm coords ([1dd4586](https://github.com/agrc/lsli-skid/commit/1dd4586a2d1e15e3d07062781cfc47896ed41bea))
* time is not an allowable agol field name ([4f96530](https://github.com/agrc/lsli-skid/commit/4f965301d3cd3fcd67a01466c271011503babdfc))


### Dependencies

* bump agrc-supervisor from 3.0.3 to 3.1.0 in the safe-dependencies group ([#6](https://github.com/agrc/lsli-skid/issues/6)) ([6f75156](https://github.com/agrc/lsli-skid/commit/6f75156dda20608baa5fe4c364070ea8842a1d28))
* bump supervisor and switch to new package name prefix ([517466f](https://github.com/agrc/lsli-skid/commit/517466fc0dfeff018466ae134656a464fc9ef3b0))


### Documentation

* note about utm flip flop in tests ([8a88840](https://github.com/agrc/lsli-skid/commit/8a888400912a9b9200458adab19caa0237bb6fc5))
* readme ([5b50d85](https://github.com/agrc/lsli-skid/commit/5b50d850496244a1614ae747ea4de3fea5640b8b))
